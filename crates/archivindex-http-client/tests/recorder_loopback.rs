//! The client contract for the recorder, checked against scripted loopback servers.

use archivindex_http_client::recorder::Recorder as Client;
use rustls::pki_types::CertificateDer;

#[path = "support/certificate.rs"]
mod certificate;
#[path = "support/client_conformance.rs"]
mod client_conformance;
#[path = "support/exact_conformance.rs"]
mod exact_conformance;
#[path = "support/proxy_conformance.rs"]
mod proxy_conformance;
#[path = "support/request.rs"]
mod request;
#[path = "support/server.rs"]
mod server;
#[path = "support/trust.rs"]
mod trust;

const PROXIED_TLS_VERSION_IS_REPORTED: bool = true;

fn client() -> Client {
    Client::new()
}

fn trusted_client(certificate: &CertificateDer<'static>) -> Client {
    client().tls_config(trust::config(certificate))
}

/// A TLS operation must recheck the deadline while assembling a record from socket reads.
#[test]
fn a_fragmented_tls_handshake_obeys_the_deadline() {
    use std::io::{Read as _, Write as _};
    use std::net::TcpListener;
    use std::time::{Duration, Instant};

    use archivindex_http_client::{Client as _, Error};

    let listener = TcpListener::bind("127.0.0.1:0").expect("a loopback listener");
    let port = listener.local_addr().unwrap().port();
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().expect("one connection");
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        assert!(socket.read(&mut [0; 8192]).expect("a ClientHello") > 0);
        socket.write_all(&[22, 3, 3, 0, 128]).unwrap();
        trickle(&mut socket, &[0; 128]);
    });
    let target = format!("https://127.0.0.1:{port}/").parse().unwrap();
    let start = Instant::now();
    let result = client().fetch_with_deadline(
        request::get(&target),
        Some(start + Duration::from_millis(500)),
    );
    let elapsed = start.elapsed();
    server.join().expect("a served handshake");

    assert!(
        matches!(result, Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::TimedOut)
    );
    assert!(elapsed < Duration::from_secs(2), "{elapsed:?}");
}

/// Fragmented encrypted data must time out before a head, or retain the head and truncate after it.
#[test]
fn fragmented_tls_response_records_obey_the_deadline() {
    use std::io::Write as _;
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use archivindex_http_client::framing::Truncation;
    use archivindex_http_client::{Client as _, Error};

    const HEAD: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 128\r\n\r\n";

    for send_head in [false, true] {
        let (certificate, config) =
            certificate::self_signed("127.0.0.1", &[&rustls::version::TLS13]);
        let listener = TcpListener::bind("127.0.0.1:0").expect("a loopback listener");
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (socket, _) = listener.accept().expect("one connection");
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let connection = rustls::ServerConnection::new(Arc::new(config)).unwrap();
            let mut stream = rustls::StreamOwned::new(connection, socket);
            server::read_request(&mut stream);
            if send_head {
                stream.write_all(HEAD).unwrap();
            }
            stream.flush().unwrap();
            // Encrypt a record without sending it through the TLS stream's buffered writer.
            stream.conn.writer().write_all(&[b'x'; 128]).unwrap();
            let mut record = Vec::new();
            stream.conn.write_tls(&mut record).unwrap();
            trickle(&mut stream.sock, &record);
        });
        let target = format!("https://127.0.0.1:{port}/").parse().unwrap();
        let start = Instant::now();
        let result = trusted_client(&certificate).fetch_with_deadline(
            request::get(&target),
            Some(start + Duration::from_millis(500)),
        );
        let elapsed = start.elapsed();
        server.join().expect("a served response");

        if send_head {
            let captured = result.expect("a retained response head");
            assert_eq!(captured.response, HEAD);
            assert_eq!(captured.truncated, Some(Truncation::Time));
        } else {
            assert!(
                matches!(result, Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::TimedOut)
            );
        }
        assert!(elapsed < Duration::from_secs(2), "{elapsed:?}");
    }
}

/// Keep each read below its idle timeout, but bound the server's lifetime if the client regresses.
fn trickle(socket: &mut std::net::TcpStream, bytes: &[u8]) {
    use std::io::Write as _;

    for byte in bytes {
        if socket.write_all(&[*byte]).is_err() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}
