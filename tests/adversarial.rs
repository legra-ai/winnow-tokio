//! Chunk-boundary tests for the generic feeder.

use std::cell::RefCell;
use std::rc::Rc;

use tokio::io::AsyncRead;
use winnow::error::{ContextError, ErrMode, Needed};
use winnow::token::take;
use winnow::{Parser, Partial};
use winnow_tokio::{Error, ModalResult, parse_stream};

fn line(input: &mut Partial<&[u8]>) -> ModalResult<Option<Vec<u8>>> {
    if input.is_empty() {
        return Err(ErrMode::Incomplete(Needed::Unknown));
    }
    let first = input[0];
    if first == b' ' || first == b'\t' || first == b'\n' || first == b'\r' {
        take::<_, _, ErrMode<ContextError>>(1usize).parse_next(input)?;
        return Ok(None);
    }
    let body =
        winnow::token::take_till::<_, _, ErrMode<ContextError>>(0.., b'\n').parse_next(input)?;
    let body = body.to_vec();
    b'\n'.parse_next(input)?;
    Ok(Some(body))
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

impl AsyncRead for ChunkedReader {
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

async fn collect_lines(data: &[u8], chunk: usize) -> Result<Vec<Vec<u8>>, Error<String>> {
    let items = Rc::new(RefCell::new(Vec::new()));
    let callback_items = Rc::clone(&items);
    parse_stream(
        ChunkedReader::new(data.to_vec(), chunk),
        line,
        move |item| {
            let callback_items = Rc::clone(&callback_items);
            async move {
                callback_items.borrow_mut().push(item);
                Ok(())
            }
        },
    )
    .await?;
    Ok(Rc::try_unwrap(items).unwrap().into_inner())
}

#[tokio::test]
async fn parses_lines_across_every_small_chunk_boundary() {
    let data = b"first\nsecond\nthird\n";
    for chunk in [1, 2, 3, 5, 8, 64] {
        let lines = collect_lines(data, chunk).await.unwrap();
        assert_eq!(
            lines,
            vec![b"first".to_vec(), b"second".to_vec(), b"third".to_vec()],
            "chunk={chunk}"
        );
    }
}

#[tokio::test]
async fn accepts_a_record_larger_than_the_read_chunk() {
    let mut data = vec![b'x'; 10_000];
    data.push(b'\n');
    let lines = collect_lines(&data, 3).await.unwrap();
    assert_eq!(lines, vec![vec![b'x'; 10_000]]);
}

#[tokio::test]
async fn callback_state_can_be_captured() {
    let count = Rc::new(RefCell::new(0));
    let callback_count = Rc::clone(&count);
    parse_stream(
        ChunkedReader::new(b"a\nb\n".to_vec(), 1),
        line,
        move |_item| {
            *callback_count.borrow_mut() += 1;
            async { Ok::<(), String>(()) }
        },
    )
    .await
    .unwrap();
    assert_eq!(*count.borrow(), 2);
}
