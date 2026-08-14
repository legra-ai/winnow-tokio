#![doc = include_str!("../README.md")]

use std::fmt;

use tokio::io::{AsyncRead, AsyncReadExt};
use winnow::error::ErrMode;
pub use winnow::{ModalResult, Partial};

/// Size of a single read from the underlying [`AsyncRead`].
///
/// The internal buffer can grow larger than this while one item spans
/// multiple chunks. Its memory use is bounded by the longest item plus one
/// chunk of read-ahead.
pub const CHUNK_SIZE: usize = 8 * 1024;

/// Errors reported by [`parse_stream`].
#[derive(Debug)]
pub enum Error<E> {
    /// The underlying reader failed at the given byte offset.
    Io {
        /// Number of input bytes successfully consumed before the failure.
        offset: usize,
        /// The reader error.
        source: std::io::Error,
    },
    /// The parser could not make valid progress.
    Parse {
        /// Byte offset at which parsing stopped.
        offset: usize,
        /// Human-readable parser diagnostic.
        detail: String,
    },
    /// The item callback rejected an emitted item.
    Callback(E),
}

impl<E: fmt::Display> fmt::Display for Error<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { offset, source } => {
                write!(formatter, "I/O error at offset {offset}: {source}")
            }
            Self::Parse { offset, detail } => {
                write!(formatter, "parse error at offset {offset}: {detail}")
            }
            Self::Callback(error) => write!(formatter, "item callback failed: {error}"),
        }
    }
}

impl<E: fmt::Debug + fmt::Display> std::error::Error for Error<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Parse { .. } | Self::Callback(_) => None,
        }
    }
}

/// Drive a winnow parser over a chunked [`AsyncRead`], invoking `on_item`
/// for each item the parser yields.
///
/// The parser receives a [`Partial`] byte slice and must return
/// [`ModalResult<Option<Item>>`]:
///
/// - `Ok(Some(item))` emits an item and must consume input.
/// - `Ok(None)` consumes input without emitting an item, for example when
///   skipping whitespace or comments.
/// - `Err(ErrMode::Incomplete(_))` asks the feeder to read more input.
/// - Any other parser error terminates the feeder with an offset-aware
///   [`Error::Parse`].
///
/// At EOF, an incomplete parser result, a non-consuming successful result, or
/// unconsumed trailing bytes is reported as a parse error. An empty input is
/// valid and returns `Ok(())`.
///
/// The feeder drains consumed bytes after each round. Memory therefore scales
/// with the longest item that has not completed, rather than with the total
/// input size.
///
/// # Errors
///
/// Returns [`Error::Io`] when reading fails, [`Error::Parse`] when the parser
/// cannot make progress or rejects the input, and [`Error::Callback`] when the
/// item callback returns an error.
pub async fn parse_stream<R, P, Item, F, Fut, E>(
    reader: R,
    mut parser: P,
    mut on_item: F,
) -> Result<(), Error<E>>
where
    R: AsyncRead + Unpin,
    P: for<'a> FnMut(&mut Partial<&'a [u8]>) -> ModalResult<Option<Item>>,
    F: FnMut(Item) -> Fut,
    Fut: Future<Output = Result<(), E>>,
    E: fmt::Debug + fmt::Display,
{
    let mut reader = reader;
    let mut buffer: Vec<u8> = Vec::with_capacity(CHUNK_SIZE * 2);
    let mut stream_offset: usize = 0;
    let mut eof = false;

    loop {
        if !eof {
            let start = buffer.len();
            buffer.resize(start + CHUNK_SIZE, 0);
            let read = reader
                .read(&mut buffer[start..])
                .await
                .map_err(|source| Error::Io {
                    offset: stream_offset,
                    source,
                })?;
            buffer.truncate(start + read);
            if read == 0 {
                eof = true;
            }
        }

        let mut consumed_this_round: usize = 0;
        loop {
            if consumed_this_round >= buffer.len() {
                break;
            }
            let slice = &buffer[consumed_this_round..];
            let mut partial = Partial::new(slice);
            let before_len = slice.len();
            match parser(&mut partial) {
                Ok(item_opt) => {
                    let consumed = before_len - partial.len();
                    if consumed == 0 {
                        return Err(Error::Parse {
                            offset: stream_offset + consumed_this_round,
                            detail: "parser returned Ok without consuming input".to_owned(),
                        });
                    }
                    consumed_this_round += consumed;
                    if let Some(item) = item_opt {
                        on_item(item).await.map_err(Error::Callback)?;
                    }
                }
                Err(ErrMode::Incomplete(_)) => {
                    if eof {
                        return Err(Error::Parse {
                            offset: stream_offset + consumed_this_round,
                            detail: format!(
                                "unexpected end of input: parser needs more bytes (remaining {} bytes)",
                                slice.len()
                            ),
                        });
                    }
                    break;
                }
                Err(ErrMode::Backtrack(error) | ErrMode::Cut(error)) => {
                    return Err(Error::Parse {
                        offset: stream_offset + consumed_this_round,
                        detail: render_context_error(&error),
                    });
                }
            }
        }

        if consumed_this_round > 0 {
            buffer.drain(..consumed_this_round);
            stream_offset += consumed_this_round;
        }

        if eof {
            if buffer.is_empty() {
                return Ok(());
            }
            if consumed_this_round == 0 {
                return Err(Error::Parse {
                    offset: stream_offset,
                    detail: format!(
                        "trailing bytes at end of input: {} bytes remain",
                        buffer.len()
                    ),
                });
            }
        }
    }
}

fn render_context_error(error: &winnow::error::ContextError) -> String {
    error.to_string().replace('\n', "; ")
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use winnow::Parser;
    use winnow::ascii::digit1;
    use winnow::error::ContextError;
    use winnow::token::{one_of, take_till};

    use super::*;

    fn int_line(input: &mut Partial<&[u8]>) -> ModalResult<Option<Vec<u8>>> {
        let digits: &[u8] = digit1::<_, ErrMode<ContextError>>.parse_next(input)?;
        let digits = digits.to_vec();
        b'\n'.parse_next(input)?;
        Ok(Some(digits))
    }

    fn comment_or_int_line(input: &mut Partial<&[u8]>) -> ModalResult<Option<Vec<u8>>> {
        let slice: &[u8] = input;
        if let Some(&b'#') = slice.first() {
            one_of::<_, _, ErrMode<ContextError>>(b"#").parse_next(input)?;
            take_till::<_, _, ErrMode<ContextError>>(0.., b'\n').parse_next(input)?;
            b'\n'.parse_next(input)?;
            return Ok(None);
        }
        int_line(input)
    }

    #[tokio::test]
    async fn emits_items_across_chunk_boundaries() {
        let input = b"12\n34\n567\n8\n".to_vec();
        let mut items = Vec::new();
        let reader = ChunkedReader::new(input, 3);
        parse_stream(reader, int_line, |item| {
            items.push(item);
            async { Ok::<(), String>(()) }
        })
        .await
        .unwrap();
        assert_eq!(
            items,
            vec![
                b"12".to_vec(),
                b"34".to_vec(),
                b"567".to_vec(),
                b"8".to_vec()
            ]
        );
    }

    #[tokio::test]
    async fn reports_error_on_midstream_eof() {
        let reader = Cursor::new(b"12\n3".to_vec());
        let error = parse_stream(reader, int_line, |_item| async { Ok::<(), String>(()) })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("unexpected end of input"));
    }

    #[tokio::test]
    async fn reports_syntax_error_with_offset() {
        let reader = Cursor::new(b"12\nXX\n".to_vec());
        let mut items = Vec::new();
        let error = parse_stream(reader, int_line, |item| {
            items.push(item);
            async { Ok::<(), String>(()) }
        })
        .await
        .unwrap_err();
        assert!(error.to_string().contains("offset 3"));
        assert_eq!(items, vec![b"12"]);
    }

    #[tokio::test]
    async fn none_items_consume_without_emit() {
        let input = b"# top comment\n42\n# another\n7\n".to_vec();
        let mut items = Vec::new();
        let reader = ChunkedReader::new(input, 5);
        parse_stream(reader, comment_or_int_line, |item| {
            items.push(item);
            async { Ok::<(), String>(()) }
        })
        .await
        .unwrap();
        assert_eq!(items, vec![b"42".to_vec(), b"7".to_vec()]);
    }

    #[tokio::test]
    async fn empty_input_is_ok() {
        let mut items = Vec::new();
        parse_stream(&[][..], int_line, |item| {
            items.push(item);
            async { Ok::<(), String>(()) }
        })
        .await
        .unwrap();
        assert_eq!(items, Vec::<Vec<u8>>::new());
    }

    #[tokio::test]
    async fn callback_error_is_preserved() {
        let reader = Cursor::new(b"1\n2\n".to_vec());
        let mut count = 0;
        let error = parse_stream(reader, int_line, |_item| {
            count += 1;
            let result = if count == 2 {
                Err("downstream rejected item".to_owned())
            } else {
                Ok(())
            };
            async move { result }
        })
        .await
        .unwrap_err();
        assert!(matches!(error, Error::Callback(message) if message == "downstream rejected item"));
    }

    #[tokio::test]
    async fn items_larger_than_chunk_still_parse() {
        let line: Vec<u8> = (0..100).map(|_| b'5').collect();
        let mut input = Vec::new();
        input.extend_from_slice(&line);
        input.push(b'\n');
        input.extend_from_slice(&line);
        input.push(b'\n');
        let reader = ChunkedReader::new(input, 7);
        let mut items = Vec::new();
        parse_stream(reader, int_line, |item| {
            items.push(item);
            async { Ok::<(), String>(()) }
        })
        .await
        .unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].len(), 100);
        assert_eq!(items[1].len(), 100);
    }

    struct ChunkedReader {
        data: Vec<u8>,
        position: usize,
        chunk: usize,
    }

    impl ChunkedReader {
        fn new(data: Vec<u8>, chunk: usize) -> Self {
            Self {
                data,
                position: 0,
                chunk,
            }
        }
    }

    impl tokio::io::AsyncRead for ChunkedReader {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
            buffer: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let remaining = self.data.len() - self.position;
            if remaining == 0 {
                return std::task::Poll::Ready(Ok(()));
            }
            let amount = buffer.remaining().min(self.chunk).min(remaining);
            let position = self.position;
            buffer.put_slice(&self.data[position..position + amount]);
            self.position += amount;
            std::task::Poll::Ready(Ok(()))
        }
    }
}
