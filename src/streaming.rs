//! Streaming (batched) parsing across the FFI boundary.
//!
//! Upstream's `BatchStream` borrows its `Parser` — a lifetime no pyclass can
//! hold. Rather than self-referential ownership, the parse runs on a
//! dedicated producer thread that owns an `Arc<Parser>` clone and the input;
//! batches cross a small bounded channel. That sidesteps the lifetime
//! entirely and buys pipeline parallelism for free: Rust parses the next
//! batch while Python processes the previous one.
//!
//! GIL discipline is the load-bearing subtlety in this module:
//!
//! - `RecordBatchStream.__next__` detaches from the interpreter while
//!   blocked on the channel, so a `FileLike` producer — whose `read()`
//!   re-attaches for every call — can always make progress.
//! - `parse_single_table` exports a native `pyarrow.RecordBatchReader`
//!   whose `get_next` is driven from pyarrow's own code; we must not assume
//!   it releases the GIL while waiting. Its producer therefore never needs
//!   the GIL: paths and byte buffers stream directly, and file-like objects
//!   are slurped into memory up front (see `ThreadInput::from_input`).

use std::fs::File;
use std::io::{BufReader, Read};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex};

use arrow::array::{RecordBatch, RecordBatchReader};
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::pyarrow::{IntoPyArrow, ToPyArrow};
use pyo3::prelude::*;
use xml2arrow::{BatchOptions, BatchStream, EventSource, Parser, TableBatch};

use crate::file_like::PyBinaryFile;
use crate::{XmlInput, XmlReader};

/// What the producer thread sends: a named batch, or the error that ended
/// the parse. Upstream's error type crosses the thread intact, so the
/// established `From<Error> for PyErr` mapping — including recovery of the
/// original Python exception from a failed file-like `read()` — still
/// applies on the consumer side.
type StreamMsg = Result<(Arc<str>, RecordBatch), xml2arrow::Error>;

/// How many batches may sit between producer and consumer. Deliberately
/// small: each batch is already up to `max_bytes_per_batch`, so the batch
/// options — not the channel — are the memory knob. The bound also caps how
/// long a producer can keep running after the consumer disappears (its next
/// `send` fails).
const CHANNEL_BOUND: usize = 2;

/// The input as the producer thread owns it — nothing here borrows Python
/// state, so the whole enum is `Send`.
pub(crate) enum ThreadInput {
    /// `bytes`/`bytearray` become one owned copy. Zero-copy would tie the
    /// parse to a GIL-bound borrow of the Python object; for a *streaming*
    /// parse the batches, not the input, are the memory concern.
    Buffer(Vec<u8>),
    File(File),
    FileLike(PyBinaryFile),
}

impl ThreadInput {
    /// `slurp_file_like` exists for `parse_single_table`: its consumer may
    /// wait on the channel while holding the GIL inside pyarrow, so its
    /// producer must never need the GIL — which a file-like `read()` does.
    /// Reading the whole object here (GIL held, before any thread exists)
    /// removes the deadlock by construction.
    pub(crate) fn from_input(input: XmlInput<'_>, slurp_file_like: bool) -> PyResult<Self> {
        Ok(match input {
            XmlInput::Bytes(b) => Self::Buffer(b.as_bytes().to_vec()),
            XmlInput::OwnedBytes(v) => Self::Buffer(v),
            XmlInput::File(f) => Self::File(f),
            XmlInput::FileLike(mut f) => {
                if slurp_file_like {
                    let mut buf = Vec::new();
                    // An io::Error wrapping a PyErr converts back to the
                    // original exception (pyo3's From<io::Error> recovers it).
                    f.read_to_end(&mut buf)?;
                    Self::Buffer(buf)
                } else {
                    Self::FileLike(f)
                }
            }
        })
    }
}

/// Spawns the parse onto its own thread and returns the consuming end.
///
/// The thread feeds `tx` until the stream ends, errors, or the receiver is
/// dropped (early consumer exit — the next `send` fails and the thread winds
/// down promptly thanks to the small channel bound).
pub(crate) fn spawn_producer(
    parser: Arc<Parser>,
    input: ThreadInput,
    options: BatchOptions,
) -> PyResult<Receiver<StreamMsg>> {
    let (tx, rx) = sync_channel(CHANNEL_BOUND);
    std::thread::Builder::new()
        .name("xml2arrow-stream".into())
        .spawn(move || match input {
            ThreadInput::Buffer(buf) => pump(parser.parse_batches_slice(&buf, options), &tx),
            ThreadInput::File(f) => pump(
                parser.parse_batches(BufReader::new(XmlReader::File(f)), options),
                &tx,
            ),
            ThreadInput::FileLike(f) => pump(
                parser.parse_batches(BufReader::new(XmlReader::FileLike(f)), options),
                &tx,
            ),
        })
        .map_err(PyErr::from)?;
    Ok(rx)
}

/// Forwards every stream item into the channel. Stops on the first send
/// failure (receiver dropped) or after forwarding an error — the upstream
/// stream fuses after yielding one, so there is nothing more to read.
fn pump<S: EventSource>(stream: BatchStream<'_, S>, tx: &SyncSender<StreamMsg>) {
    for item in stream {
        let msg = item.map(|TableBatch { table, batch }| (table, batch));
        let is_err = msg.is_err();
        if tx.send(msg).is_err() || is_err {
            return;
        }
    }
}

/// A Python iterator over `(table_name, pyarrow.RecordBatch)` pairs.
///
/// Returned by `XmlToArrowParser.parse_batches`. Exhausting it, dropping it,
/// or hitting an error all shut the producer thread down; after an error or
/// exhaustion the iterator only raises `StopIteration`.
#[pyclass(frozen, name = "RecordBatchStream", module = "xml2arrow")]
pub struct RecordBatchStream {
    /// `Option` so termination can drop the channel (fusing the iterator and
    /// unblocking the producer); `Mutex` because pyclasses must be `Sync`
    /// and `Receiver` is not. The receiver is taken *out* around the
    /// blocking `recv` so the closure handed to `py.detach` owns it and is
    /// therefore `Send`.
    rx: Mutex<Option<Receiver<StreamMsg>>>,
}

impl RecordBatchStream {
    pub(crate) fn new(rx: Receiver<StreamMsg>) -> Self {
        Self {
            rx: Mutex::new(Some(rx)),
        }
    }
}

#[pymethods]
impl RecordBatchStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<(String, Py<PyAny>)>> {
        // Taking the receiver also serializes concurrent __next__ calls: a
        // second caller finds None and stops, rather than blocking on a
        // channel someone else is draining.
        let Some(rx) = self.rx.lock().unwrap().take() else {
            return Ok(None);
        };
        // Detach while blocked so a FileLike producer can attach for its
        // read() calls, and so other Python threads keep running. The
        // closure must *own* the receiver (`&Receiver` is not `Send`, which
        // `detach` requires), so it hands it back alongside the message.
        let (msg, rx) = py.detach(move || {
            let msg = rx.recv();
            (msg, rx)
        });
        match msg {
            Ok(Ok((name, batch))) => {
                *self.rx.lock().unwrap() = Some(rx);
                Ok(Some((name.to_string(), batch.to_pyarrow(py)?.unbind())))
            }
            // The parse failed and the upstream stream fused; leaving the
            // receiver dropped makes every later call a clean StopIteration.
            Ok(Err(e)) => Err(e.into()),
            // Producer finished cleanly and hung up.
            Err(_) => Ok(None),
        }
    }

    fn __repr__(&self) -> String {
        let state = if self.rx.lock().unwrap().is_some() {
            "active"
        } else {
            "exhausted"
        };
        format!("RecordBatchStream({state})")
    }
}

/// Bridges the channel to arrow's `RecordBatchReader` so the parse can cross
/// to Python as a native `pyarrow.RecordBatchReader` through the Arrow C
/// stream interface.
struct ChannelBatchReader {
    schema: SchemaRef,
    rx: Receiver<StreamMsg>,
    /// Once an error is delivered the stream is over; report end-of-stream
    /// afterwards instead of recv-ing on a channel whose producer is gone.
    done: bool,
}

impl Iterator for ChannelBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.rx.recv() {
            // The config has exactly one output table (validated before the
            // producer was spawned), so every message is that table's.
            Ok(Ok((_, batch))) => Some(Ok(batch)),
            Ok(Err(e)) => {
                self.done = true;
                Some(Err(match e {
                    xml2arrow::Error::Arrow(e) => e,
                    e => ArrowError::ExternalError(Box::new(e)),
                }))
            }
            Err(_) => {
                self.done = true;
                None
            }
        }
    }
}

impl RecordBatchReader for ChannelBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Implementation of `XmlToArrowParser.parse_batches`.
pub(crate) fn parse_batches_impl(
    parser: &Arc<Parser>,
    input: XmlInput<'_>,
    options: BatchOptions,
) -> PyResult<RecordBatchStream> {
    let input = ThreadInput::from_input(input, false)?;
    let rx = spawn_producer(parser.clone(), input, options)?;
    Ok(RecordBatchStream::new(rx))
}

/// Implementation of `XmlToArrowParser.parse_single_table`.
pub(crate) fn parse_single_table_impl(
    py: Python<'_>,
    parser: &Arc<Parser>,
    input: XmlInput<'_>,
    options: BatchOptions,
) -> PyResult<Py<PyAny>> {
    // Validate the config shape and take the schema BEFORE spawning
    // anything: a multi-table config must raise InvalidConfigError here,
    // not from the first read_next_batch deep inside pyarrow.
    let schema = parser.single_table_schema()?;
    let input = ThreadInput::from_input(input, true)?;
    let rx = spawn_producer(parser.clone(), input, options)?;
    let reader: Box<dyn RecordBatchReader + Send> = Box::new(ChannelBatchReader {
        schema,
        rx,
        done: false,
    });
    Ok(reader.into_pyarrow(py)?.unbind())
}
