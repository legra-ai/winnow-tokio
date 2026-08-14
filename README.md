# winnow-tokio

[![Crates.io](https://img.shields.io/crates/v/winnow-tokio.svg)](https://crates.io/crates/winnow-tokio)
[![Documentation](https://docs.rs/winnow-tokio/badge.svg)](https://docs.rs/winnow-tokio)
[![CI](https://github.com/legra-ai/winnow-tokio/actions/workflows/ci.yml/badge.svg)](https://github.com/legra-ai/winnow-tokio/actions/workflows/ci.yml)
[![License](https://img.shields.io/crates/l/winnow-tokio.svg)](https://github.com/legra-ai/winnow-tokio/blob/main/LICENSE-APACHE)

Drive incremental [`winnow`](https://docs.rs/winnow) parsers from Tokio
[`AsyncRead`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncRead.html)
sources without loading the complete input into memory.

## What it does

`parse_stream` is a small adapter between an asynchronous byte source and a
parser that understands winnow's [`Partial`](https://docs.rs/winnow/latest/winnow/stream/struct.Partial.html)
input:

1. Read a fixed-size chunk from an `AsyncRead`.
2. Give the parser the available bytes.
3. Read more when the parser returns `ErrMode::Incomplete`.
4. Emit completed items immediately through an async callback.
5. Drain consumed bytes before continuing.

The parser can consume input without emitting an item, which is useful for
whitespace, comments, directives, and other non-semantic records. A parser
that reports success without consuming input is rejected immediately so a
bug cannot create an infinite loop.

## Example

```rust
use winnow::ascii::digit1;
use winnow::error::ContextError;
use winnow::Parser;
use winnow_tokio::{parse_stream, ModalResult, Partial};

fn integer_line(input: &mut Partial<&[u8]>) -> ModalResult<Option<u64>> {
    let digits: &[u8] = digit1::<_, winnow::error::ErrMode<ContextError>>
        .parse_next(input)?;
    let value = std::str::from_utf8(digits)
        .expect("digit1 only accepts ASCII digits")
        .parse()
        .expect("the digit sequence must fit in u64");
    b'\n'.parse_next(input)?;
    Ok(Some(value))
}

# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), winnow_tokio::Error<std::convert::Infallible>> {
let input = std::io::Cursor::new(b"12\n34\n".to_vec());
let mut values = Vec::new();
winnow_tokio::parse_stream(input, integer_line, |value| {
    values.push(value);
    async { Ok::<(), std::convert::Infallible>(()) }
})
.await?;
assert_eq!(values, [12, 34]);
# Ok(())
# }
```

## Memory behavior

The feeder does not accumulate the complete stream. It retains only consumed
read-ahead plus the currently incomplete item, so memory scales with the
longest item rather than total input size. A single item may exceed
`CHUNK_SIZE`; the buffer grows only as needed to finish that item.

## Scope

This crate does not define a grammar, token model, or wire format. It is a
runtime adapter for applications that already use winnow for incremental
parsing over Tokio byte streams.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE)
  or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](LICENSE-MIT)
  or <https://opensource.org/licenses/MIT>)

at your option.
