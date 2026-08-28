//! Wiremock scripting and byte recording across actual TCP connections.

use std::io::{Read, Write};
use std::net::{Ipv4Addr, TcpStream};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::Duration;

use archivindex_test_support::http::proxy::RecordingProxy;
use archivindex_test_support::http::{RequestExt as _, response, response_bytes, serve_with};

fn send(port: u16, request: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut stream = TcpStream::connect((Ipv4Addr::LOCALHOST, port))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;
    stream.write_all(request)?;
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes)?;
    Ok(bytes)
}

#[test]
fn records_exact_bytes_while_wiremock_decodes_requests() -> std::io::Result<()> {
    let body: Vec<_> = (0u8..=255).cycle().take(32_768).collect();
    let reply = response(200, &[("set-cookie", "a=1"), ("set-cookie", "b=2")], &body);
    let server = serve_with(1, move |request| (reply.clone(), request.clone()))?;
    let proxy = RecordingProxy::start(server.port(), |_, _| {})?;
    let mut request = b"POST /a%2Fb?q=a%7Cb HTTP/1.1\r\nHost: localhost\r\nX-MiXeD: first\r\nx-mixed: second\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n8000\r\n".to_vec();
    request.extend_from_slice(&body);
    request.extend_from_slice(b"\r\n0\r\n\r\n");
    let received = send(proxy.port(), &request)?;
    let exchanges = proxy.finish()?;
    let requests = server.finish();
    assert_eq!(exchanges.len(), 1);
    assert_eq!(exchanges[0].request, request);
    assert_eq!(exchanges[0].response, received);
    assert!(received.ends_with(&body));
    let head = String::from_utf8_lossy(&received[..received.len() - body.len()]);
    assert!(head.contains("set-cookie: a=1\r\n"));
    assert!(head.contains("set-cookie: b=2\r\n"));
    assert_eq!(requests[0].path(), "/a%2Fb?q=a%7Cb");
    assert_eq!(requests[0].header("X-MIXED"), Some("first"));
    assert_eq!(requests[0].body, body);
    Ok(())
}

#[test]
fn records_response_faults_after_serialization() -> std::io::Result<()> {
    let server = serve_with(1, |_| (response(200, &[], "complete"), ()))?;
    let proxy = RecordingProxy::start(server.port(), |_, bytes| bytes.truncate(bytes.len() - 3))?;
    let received = send(proxy.port(), b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")?;
    let exchanges = proxy.finish()?;
    let _ = server.finish();
    assert_eq!(exchanges[0].response, received);
    let text = String::from_utf8(received).unwrap();
    assert!(text.contains("content-length: 8\r\n"));
    assert!(text.ends_with("\r\n\r\ncompl"));
    Ok(())
}

#[test]
fn serializes_reproducible_fixtures() -> std::io::Result<()> {
    let template = response(200, &[("content-type", "text/plain")], "hello");
    assert_eq!(response_bytes(template.clone())?, response_bytes(template)?);
    Ok(())
}

#[test]
fn missing_requests_fail_without_waiting_for_connections() -> std::io::Result<()> {
    let server = serve_with(1, |_| (response(200, &[], ""), ()))?;
    assert!(catch_unwind(AssertUnwindSafe(|| server.finish())).is_err());
    Ok(())
}

#[test]
fn script_panics_reach_the_test_thread_without_breaking_wiremock() -> std::io::Result<()> {
    let server = serve_with::<()>(1, |_| panic!("script assertion"))?;
    let received = send(server.port(), b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")?;
    assert!(received.starts_with(b"HTTP/1.1 500 "));
    let panic = catch_unwind(AssertUnwindSafe(|| server.finish())).unwrap_err();
    assert_eq!(panic.downcast_ref::<&str>(), Some(&"script assertion"));
    assert!(response_bytes(response(200, &[], "still running"))?.ends_with(b"still running"));
    Ok(())
}
