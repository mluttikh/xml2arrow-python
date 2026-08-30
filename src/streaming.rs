//! Streaming (batched) parsing across the FFI boundary.
//!
//! A pyclass here holds the upstream `BatchStream` directly and pulls from it
//! on the calling thread. That is only possible because the stream *owns* its
//! parser (`Parser::into_batches`); when it borrowed one, no pyclass could hold
//! it, and this module ran the parse on a producer thread feeding a bounded
//! channel purely to escape the lifetime.
//!
//! Dropping the thread removed more than the thread. Two things it had forced:
//!
//! - `parse_single_table` used to read file-like objects fully into memory
//!   before parsing. pyarrow drives the returned reader from its own code and
//!   may hold the interpreter while doing so, and the producer thread needed
//!   the interpreter to call `read()` — a deadlock, avoided by removing the
//!   need to call `read()` at all. Parsing on the calling thread makes the
//!   re-entrant attach trivially safe, so a file-like object now streams like
//!   any other input instead of being materialised.
//! - Producer panics had to be caught, given a message, and re-raised, because
//!   a panicked thread and a finished one both look like a closed channel.
//!   Without a thread, a panic propagates the way every other pyo3 call does.
//!
//! What remains is one GIL rule: `__next__` detaches from the interpreter while
//! parsing, so other Python threads keep running. A file-like input re-attaches
//! inside `read()`, which is sound from a detached state and from an attached
//! one alike.

use std::io::{BufReader, Cursor};
use std::sync::Mutex;

use arrow::array::RecordBatchReader;
use arrow::pyarrow::{IntoPyArrow, ToPyArrow};
use pyo3::prelude::*;
use xml2arrow::{BatchOptions, BatchStream, Parser, ReaderSource, TableBatch};

use crate::{XmlInput, XmlReader};

/// The single stream type this module deals in.
///
/// Every input is funnelled through `XmlReader` so there is one type to name
/// rather than one per input kind. In-memory `bytes` therefore stream through a
/// cursor instead of upstream's zero-copy slice path: a stream that borrowed
/// the buffer could not own it, and for a *streaming* parse the batches, not
/// the input, are the memory concern. `parse()` keeps the zero-copy path.
type OwnedStream = BatchStream<'static, ReaderSource<BufReader<XmlReader>>>;

/// Wraps any accepted input as the reader the stream will own.
///
/// `bytes`/`bytearray` are copied once, as they were when the producer thread
/// owned them.
fn reader_for(input: XmlInput<'_>) -> BufReader<XmlReader> {
    BufReader::new(match input {
        XmlInput::Bytes(b) => XmlReader::Buffer(Cursor::new(b.as_bytes().to_vec())),
        XmlInput::OwnedBytes(v) => XmlReader::Buffer(Cursor::new(v)),
        XmlInput::File(f) => XmlReader::File(f),
        XmlInput::FileLike(f) => XmlReader::FileLike(f),
    })
}

/// A Python iterator over `(table_name, pyarrow.RecordBatch)` pairs.
///
/// Returned by `XmlToArrowParser.parse_batches`. Exhausting it, dropping it, or
/// hitting an error all end the parse; after an error or exhaustion the
/// iterator only raises `StopIteration`.
///
/// Iterate from a single thread: like any Python iterator this one is not
/// thread-safe, and here a concurrent `__next__` sees `StopIteration` rather
/// than the batch another thread is taking.
#[pyclass(frozen, name = "RecordBatchStream", module = "xml2arrow")]
pub struct RecordBatchStream {
    /// `Option` so termination can drop the stream, which fuses the iterator
    /// and releases the input; `Mutex` because a frozen pyclass must be `Sync`.
    /// The stream is taken *out* around the pull so the closure handed to
    /// `py.detach` owns it and is therefore `Send`.
    stream: Mutex<Option<OwnedStream>>,
}

impl RecordBatchStream {
    fn new(stream: OwnedStream) -> Self {
        Self {
            stream: Mutex::new(Some(stream)),
        }
    }
}

#[pymethods]
impl RecordBatchStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<(String, Py<PyAny>)>> {
        // Taking the stream also serializes concurrent __next__ calls: a second
        // caller finds None and stops, rather than racing for the same batch.
        let Some(mut stream) = self.stream.lock().unwrap().take() else {
            return Ok(None);
        };
        // Detach across the parse so other Python threads keep running. The
        // closure must own the stream to be `Send`, so it hands it back.
        let (item, stream) = py.detach(move || {
            let item = stream.next();
            (item, stream)
        });
        match item {
            Some(Ok(TableBatch { table, batch })) => {
                *self.stream.lock().unwrap() = Some(stream);
                Ok(Some((table.to_string(), batch.to_pyarrow(py)?.unbind())))
            }
            // The parse failed and the stream fused; leaving it dropped makes
            // every later call a clean StopIteration.
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }

    fn __repr__(&self) -> String {
        let state = if self.stream.lock().unwrap().is_some() {
            "active"
        } else {
            "exhausted"
        };
        format!("RecordBatchStream({state})")
    }
}

/// Implementation of `XmlToArrowParser.parse_batches`.
pub(crate) fn parse_batches_impl(
    parser: &Parser,
    input: XmlInput<'_>,
    options: BatchOptions,
) -> PyResult<RecordBatchStream> {
    let stream = parser.clone().into_batches(reader_for(input), options);
    Ok(RecordBatchStream::new(stream))
}

/// Implementation of `XmlToArrowParser.parse_single_table`.
pub(crate) fn parse_single_table_impl(
    py: Python<'_>,
    parser: &Parser,
    input: XmlInput<'_>,
    options: BatchOptions,
) -> PyResult<Py<PyAny>> {
    // `into_single_table` validates the config shape, so a multi-table config
    // raises here rather than from the first read_next_batch inside pyarrow.
    let reader = parser
        .clone()
        .into_single_table(reader_for(input), options)?;
    let reader: Box<dyn RecordBatchReader + Send> = Box::new(reader);
    Ok(reader.into_pyarrow(py)?.unbind())
}
