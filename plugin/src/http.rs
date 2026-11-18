//! HTTP/1.1 over a unix socket: enough of it for the engine's API.
//!
//! One connection per request, closed after it. A body is read by its length, by chunks, or to the end
//! of the connection, and can be read as it arrives: the engine's events come that way.

use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

#[derive(Debug, Clone, Copy, PartialEq)]
enum Mode {
    Chunked { remaining: usize },
    Length(usize),
    ToEof,
    Done,
}

/// A response whose head was read; the body is read with [`next`](Self::next).
pub struct Body<R> {
    reader: R,
    mode: Mode,
    pub status: u16,
}

pub type Response = Body<BufReader<UnixStream>>;

/// Reads the status line and the headers.
pub async fn read_head<R: AsyncBufRead + Unpin>(mut reader: R) -> io::Result<Body<R>> {
    let bad = |m: &str| io::Error::new(io::ErrorKind::InvalidData, m.to_string());
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    let status: u16 =
        line.split_whitespace().nth(1).and_then(|s| s.parse().ok()).ok_or_else(|| bad("not an HTTP status line"))?;
    let (mut chunked, mut length) = (false, None);
    loop {
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        let l = line.trim_end();
        if l.is_empty() {
            break;
        }
        if let Some((k, v)) = l.split_once(':') {
            let (k, v) = (k.trim().to_ascii_lowercase(), v.trim());
            if k == "transfer-encoding" && v.to_ascii_lowercase().contains("chunked") {
                chunked = true;
            } else if k == "content-length" {
                length = v.parse::<usize>().ok();
            }
        }
    }
    let mode = if status < 200 || status == 204 || status == 304 {
        Mode::Length(0)
    } else if chunked {
        Mode::Chunked { remaining: 0 }
    } else if let Some(n) = length {
        Mode::Length(n)
    } else {
        Mode::ToEof
    };
    Ok(Body { reader, mode, status })
}

impl<R: AsyncBufRead + Unpin> Body<R> {
    /// The next piece of the body, of whatever size; nothing at its end.
    pub async fn next(&mut self) -> io::Result<Option<Vec<u8>>> {
        loop {
            match self.mode {
                Mode::Done => return Ok(None),
                Mode::Length(0) => self.mode = Mode::Done,
                Mode::Length(n) => {
                    let data = read_some(&mut self.reader, n).await?;
                    self.mode = Mode::Length(n - data.len());
                    return Ok(Some(data));
                }
                Mode::ToEof => {
                    let mut buf = vec![0u8; 16384];
                    let k = self.reader.read(&mut buf).await?;
                    if k == 0 {
                        self.mode = Mode::Done;
                        return Ok(None);
                    }
                    buf.truncate(k);
                    return Ok(Some(buf));
                }
                Mode::Chunked { remaining: 0 } => {
                    let mut line = String::new();
                    if self.reader.read_line(&mut line).await? == 0 {
                        self.mode = Mode::Done;
                        return Ok(None);
                    }
                    let t = line.trim();
                    if t.is_empty() {
                        // The line end that closes the chunk before.
                        continue;
                    }
                    let size = usize::from_str_radix(t.split(';').next().unwrap_or(""), 16)
                        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "not a chunk size"))?;
                    self.mode = if size == 0 { Mode::Done } else { Mode::Chunked { remaining: size } };
                }
                Mode::Chunked { remaining } => {
                    let data = read_some(&mut self.reader, remaining).await?;
                    self.mode = Mode::Chunked { remaining: remaining - data.len() };
                    return Ok(Some(data));
                }
            }
        }
    }

    pub async fn collect(&mut self) -> io::Result<Vec<u8>> {
        let mut all = Vec::new();
        while let Some(c) = self.next().await? {
            all.extend_from_slice(&c);
        }
        Ok(all)
    }
}

async fn read_some<R: AsyncRead + Unpin>(r: &mut R, max: usize) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; max.min(16384)];
    let k = r.read(&mut buf).await?;
    if k == 0 {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    buf.truncate(k);
    Ok(buf)
}

/// The engine's socket.
#[derive(Debug, Clone)]
pub struct UnixHttp {
    path: PathBuf,
}

impl UnixHttp {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Sends a request and gives the response with its body still unread.
    pub async fn open(&self, method: &str, target: &str, body: Option<&[u8]>) -> io::Result<Response> {
        let mut s = UnixStream::connect(&self.path).await?;
        let mut head = format!(
            "{method} {target} HTTP/1.1\r\nHost: engine\r\nUser-Agent: yoke-plugin-containers\r\nAccept: application/json\r\nConnection: close\r\n"
        );
        if let Some(b) = body {
            head.push_str(&format!("Content-Type: application/json\r\nContent-Length: {}\r\n", b.len()));
        }
        head.push_str("\r\n");
        s.write_all(head.as_bytes()).await?;
        if let Some(b) = body {
            s.write_all(b).await?;
        }
        read_head(BufReader::new(s)).await
    }

    /// A whole request, within `within`: the status and the body.
    pub async fn request(&self, method: &str, target: &str, within: Duration) -> io::Result<(u16, Vec<u8>)> {
        let work = async {
            let mut r = self.open(method, target, None).await?;
            let body = r.collect().await?;
            Ok::<_, io::Error>((r.status, body))
        };
        tokio::time::timeout(within, work)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, format!("{method} {target}: no answer within {} s", within.as_secs())))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn body_of(raw: &[u8]) -> (u16, Vec<u8>) {
        let mut r = read_head(BufReader::new(raw)).await.unwrap();
        let b = r.collect().await.unwrap();
        (r.status, b)
    }

    #[tokio::test]
    async fn a_body_by_its_length() {
        let (s, b) = body_of(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nApi-Version: 1.45\r\n\r\nhello").await;
        assert_eq!((s, b.as_slice()), (200, &b"hello"[..]));
    }

    #[tokio::test]
    async fn a_body_by_chunks_with_an_extension_and_a_trailer() {
        let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n6;ext=1\r\n world\r\n0\r\nX-T: 1\r\n\r\n";
        assert_eq!(body_of(raw).await.1, b"hello world");
    }

    #[tokio::test]
    async fn no_content_and_a_body_to_the_end() {
        let (s, b) = body_of(b"HTTP/1.1 204 No Content\r\n\r\n").await;
        assert_eq!((s, b.len()), (204, 0));
        assert_eq!(body_of(b"HTTP/1.1 200 OK\r\n\r\nuntil eof").await.1, b"until eof");
    }

    #[tokio::test]
    async fn a_body_cut_short_or_a_status_line_that_is_none_is_an_error() {
        let mut r = read_head(BufReader::new(&b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nabc"[..])).await.unwrap();
        assert!(r.collect().await.is_err());
        assert!(read_head(BufReader::new(&b"nonsense\r\n\r\n"[..])).await.is_err());
    }
}
