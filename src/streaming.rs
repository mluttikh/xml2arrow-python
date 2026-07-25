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

use std::any::Any;
use std::fs::File;
use std::io::{BufReader, Read};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use arrow::array::{RecordBatch, RecordBatchReader};
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::pyarrow::{IntoPyArrow, ToPyArrow};
use pyo3::exceptions::PyRuntimeError;
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
) -> PyResult<Producer> {
    let (tx, rx) = sync_channel(CHANNEL_BOUND);
    let handle = std::thread::Builder::new()
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
    Ok(Producer {
        rx,
        handle: Some(handle),
    })
}

/// The consuming end of the channel *plus* the thread feeding it.
///
/// Keeping the `JoinHandle` is what lets a panicked producer be told apart
/// from one that finished: both drop the sender, so a bare `recv` reports the
/// same disconnect either way — and reporting a panic as a clean end of
/// stream would silently truncate the caller's data.
pub(crate) struct Producer {
    rx: Receiver<StreamMsg>,
    /// `Option` only because `JoinHandle::join` consumes it. It is taken at
    /// most once, on disconnect, when there is nothing left to pull.
    handle: Option<JoinHandle<()>>,
}

/// The outcome of one pull from the producer.
enum Pull {
    /// A named batch, or the parse error that ended the stream.
    Item(StreamMsg),
    /// The producer forwarded everything and hung up.
    Done,
    /// The producer thread panicked; carries the panic message.
    Panicked(String),
}

impl Producer {
    /// Blocks until the next message arrives or the producer disconnects.
    ///
    /// Joining on disconnect costs nothing measurable: the thread has already
    /// dropped its sender, so it is at most instants from exiting.
    fn pull(&mut self) -> Pull {
        match self.rx.recv() {
            Ok(msg) => Pull::Item(msg),
            Err(_) => match self.handle.take().map(JoinHandle::join) {
                Some(Err(payload)) => Pull::Panicked(format!(
                    "the xml2arrow parser thread panicked: {}; \
                     the stream ended early and its output is incomplete",
                    panic_message(&*payload)
                )),
                // Either a clean finish, or a second pull after the handle was
                // already consumed — both mean end of stream.
                _ => Pull::Done,
            },
        }
    }
}

/// Best-effort text of a panic payload: `panic!` yields `&str` for a literal
/// message and `String` for a formatted one; anything else is opaque.
fn panic_message(payload: &(dyn Any + Send)) -> &str {
    if let Some(s) = payload.downcast_ref::<&str>() {
        s
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s
    } else {
        "<non-string panic payload>"
    }
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
///
/// Iterate from a single thread: like any Python iterator this one is not
/// thread-safe, and here a concurrent `__next__` sees `StopIteration` rather
/// than the batch another thread is taking.
#[pyclass(frozen, name = "RecordBatchStream", module = "xml2arrow")]
pub struct RecordBatchStream {
    /// `Option` so termination can drop the channel (fusing the iterator and
    /// unblocking the producer); `Mutex` because pyclasses must be `Sync`
    /// and `Receiver` is not. The producer is taken *out* around the
    /// blocking pull so the closure handed to `py.detach` owns it and is
    /// therefore `Send`.
    producer: Mutex<Option<Producer>>,
}

impl RecordBatchStream {
    pub(crate) fn new(producer: Producer) -> Self {
        Self {
            producer: Mutex::new(Some(producer)),
        }
    }
}

#[pymethods]
impl RecordBatchStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<(String, Py<PyAny>)>> {
        // Taking the producer also serializes concurrent __next__ calls: a
        // second caller finds None and stops, rather than blocking on a
        // channel someone else is draining.
        let Some(mut producer) = self.producer.lock().unwrap().take() else {
            return Ok(None);
        };
        // Detach while blocked so a FileLike producer can attach for its
        // read() calls, and so other Python threads keep running. The closure
        // must *own* the producer — a `&Producer` is not `Send`, because the
        // `Receiver` inside it is not `Sync` — so it hands it back alongside
        // the message.
        let (pull, producer) = py.detach(move || (producer.pull(), producer));
        match pull {
            Pull::Item(Ok((name, batch))) => {
                *self.producer.lock().unwrap() = Some(producer);
                Ok(Some((name.to_string(), batch.to_pyarrow(py)?.unbind())))
            }
            // The parse failed and the upstream stream fused; leaving the
            // producer dropped makes every later call a clean StopIteration.
            Pull::Item(Err(e)) => Err(e.into()),
            Pull::Done => Ok(None),
            // A panic is a bug, not a parse failure, so it gets RuntimeError
            // rather than an Xml2ArrowError subclass — but it must be raised:
            // reporting it as exhaustion would hand back a truncated table.
            Pull::Panicked(msg) => Err(PyRuntimeError::new_err(msg)),
        }
    }

    fn __repr__(&self) -> String {
        let state = if self.producer.lock().unwrap().is_some() {
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
    /// `None` once the stream is over; an error (or a panic) ends it, and
    /// pulling again on a producer that is gone would only re-report the
    /// disconnect.
    producer: Option<Producer>,
}

impl Iterator for ChannelBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let pull = self.producer.as_mut()?.pull();
        if !matches!(pull, Pull::Item(Ok(_))) {
            self.producer = None;
        }
        match pull {
            // The config has exactly one output table (validated before the
            // producer was spawned), so every message is that table's.
            Pull::Item(Ok((_, batch))) => Some(Ok(batch)),
            Pull::Item(Err(e)) => Some(Err(match e {
                xml2arrow::Error::Arrow(e) => e,
                e => ArrowError::ExternalError(Box::new(e)),
            })),
            Pull::Done => None,
            // Surfacing the panic as an error keeps pyarrow from treating a
            // truncated stream as a complete one; there is no richer channel
            // than ArrowError across the C stream interface.
            Pull::Panicked(msg) => Some(Err(ArrowError::ExternalError(msg.into()))),
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
    let producer = spawn_producer(parser.clone(), input, options)?;
    Ok(RecordBatchStream::new(producer))
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
    let producer = spawn_producer(parser.clone(), input, options)?;
    let reader: Box<dyn RecordBatchReader + Send> = Box::new(ChannelBatchReader {
        schema,
        producer: Some(producer),
    });
    Ok(reader.into_pyarrow(py)?.unbind())
}
