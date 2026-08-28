//! SOCKS5 proxy configuration, and CONNECT negotiation (RFC 1928) with RFC 1929 username and
//! password authentication.
//!
//! Every client checks its proxy URI with [`Proxy::parse`], so they all accept the same proxies.
//! Negotiation uses the recorder's timed transport and is never included in captured HTTP bytes.

use std::io::{ErrorKind, Read, Write};
use std::net::{IpAddr, ToSocketAddrs};

use crate::InvalidProxy;

#[derive(Clone)]
pub struct Proxy {
    pub host: String,
    pub port: u16,
    remote_dns: bool,
    credentials: Option<(Vec<u8>, Vec<u8>)>,
}

impl std::fmt::Debug for Proxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Proxy")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("remote_dns", &self.remote_dns)
            .field("authenticated", &self.credentials.is_some())
            .finish()
    }
}

impl Proxy {
    pub fn parse(value: &str) -> Result<Self, InvalidProxy> {
        let url = url::Url::parse(value)?;
        let remote_dns = match url.scheme() {
            "socks5" => false,
            "socks5h" => true,
            _ => return Err(InvalidProxy::UnsupportedScheme),
        };
        if !matches!(url.path(), "" | "/") {
            return Err(InvalidProxy::UnsupportedPath);
        }
        if url.query().is_some() {
            return Err(InvalidProxy::UnsupportedQuery);
        }
        if url.fragment().is_some() {
            return Err(InvalidProxy::UnsupportedFragment);
        }
        let host = url
            .host_str()
            .filter(|host| !host.is_empty())
            .ok_or(InvalidProxy::MissingHost)?;
        let host = host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_owned();
        let port = url.port().unwrap_or(1080);
        if port == 0 {
            return Err(InvalidProxy::ZeroPort);
        }
        let uri = fluent_uri::Uri::parse(url.as_str())?;
        let credentials = uri
            .authority()
            .and_then(|authority| authority.userinfo())
            .map(|userinfo| {
                let (user, password) = userinfo
                    .split_once(':')
                    .ok_or(InvalidProxy::MissingPassword)?;
                let user = user.decode().to_bytes().into_owned();
                let password = password.decode().to_bytes().into_owned();
                if !(1..=255).contains(&user.len()) {
                    Err(InvalidProxy::InvalidUsernameLength)
                } else if !(1..=255).contains(&password.len()) {
                    Err(InvalidProxy::InvalidPasswordLength)
                } else {
                    Ok((user, password))
                }
            })
            .transpose()?;
        Ok(Self {
            host,
            port,
            remote_dns,
            credentials,
        })
    }

    pub fn tunnel(
        &self,
        stream: &mut (impl Read + Write),
        host: &str,
        port: u16,
    ) -> std::io::Result<()> {
        // Resolve locally only when requested, never as a fallback for remote DNS.
        let address = match host.parse::<IpAddr>() {
            Ok(address) => Some(address),
            Err(_) if self.remote_dns => None,
            Err(_) => Some(
                (host, port)
                    .to_socket_addrs()?
                    .next()
                    .ok_or_else(|| {
                        std::io::Error::new(
                            ErrorKind::NotFound,
                            "the host resolved to no addresses",
                        )
                    })?
                    .ip(),
            ),
        };
        let mut request = vec![5, 1, 0];
        match address {
            Some(IpAddr::V4(address)) => {
                request.push(1);
                request.extend_from_slice(&address.octets());
            }
            Some(IpAddr::V6(address)) => {
                request.push(4);
                request.extend_from_slice(&address.octets());
            }
            None => {
                let length = u8::try_from(host.len()).map_err(|_| {
                    std::io::Error::new(ErrorKind::InvalidInput, "SOCKS hostname exceeds 255 bytes")
                })?;
                request.extend_from_slice(&[3, length]);
                request.extend_from_slice(host.as_bytes());
            }
        }
        request.extend_from_slice(&port.to_be_bytes());

        let method = if self.credentials.is_some() { 2 } else { 0 };
        stream.write_all(&[5, 1, method])?;
        let mut selection = [0; 2];
        stream.read_exact(&mut selection)?;
        if selection != [5, method] {
            return Err(std::io::Error::new(
                ErrorKind::PermissionDenied,
                "SOCKS authentication method rejected",
            ));
        }
        if let Some((user, password)) = &self.credentials {
            // Both lengths were checked when parsing the proxy configuration.
            let mut auth = vec![
                1,
                u8::try_from(user.len()).expect("validated username length"),
            ];
            auth.extend_from_slice(user);
            auth.push(u8::try_from(password.len()).expect("validated password length"));
            auth.extend_from_slice(password);
            stream.write_all(&auth)?;
            stream.read_exact(&mut selection)?;
            if selection != [1, 0] {
                return Err(std::io::Error::new(
                    ErrorKind::PermissionDenied,
                    "SOCKS authentication failed",
                ));
            }
        }
        stream.write_all(&request)?;
        let mut reply = [0; 4];
        stream.read_exact(&mut reply)?;
        if reply[0] != 5 || reply[2] != 0 {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "invalid SOCKS reply",
            ));
        }
        if reply[1] != 0 {
            return Err(std::io::Error::new(
                ErrorKind::ConnectionRefused,
                format!("SOCKS CONNECT failed (status {})", reply[1]),
            ));
        }
        // BND.ADDR identifies the proxy's bound socket, not the origin. Consume it without
        // recording it as the origin address. Domain replies need not be valid UTF-8.
        let length = match reply[3] {
            1 => 4,
            4 => 16,
            3 => {
                let mut length = [0];
                stream.read_exact(&mut length)?;
                usize::from(length[0])
            }
            _ => {
                return Err(std::io::Error::new(
                    ErrorKind::InvalidData,
                    "invalid SOCKS address type",
                ));
            }
        };
        let mut bound = [0; 257];
        stream.read_exact(&mut bound[..length + 2])
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::Proxy;
    use crate::InvalidProxy;

    #[test]
    fn configuration_errors_identify_invalid_components() {
        for (uri, expected) in [
            ("http://localhost:1080", InvalidProxy::UnsupportedScheme),
            ("socks4://localhost:1080", InvalidProxy::UnsupportedScheme),
            ("socks5h://", InvalidProxy::MissingHost),
            ("socks5h://localhost:0", InvalidProxy::ZeroPort),
            ("socks5h://localhost/path", InvalidProxy::UnsupportedPath),
            ("socks5h://localhost?query", InvalidProxy::UnsupportedQuery),
            (
                "socks5h://localhost#fragment",
                InvalidProxy::UnsupportedFragment,
            ),
            ("socks5h://secret@localhost", InvalidProxy::MissingPassword),
            ("socks5h://secret:@localhost", InvalidProxy::MissingPassword),
            (
                "socks5h://:secret@localhost",
                InvalidProxy::InvalidUsernameLength,
            ),
        ] {
            let error = Proxy::parse(uri).unwrap_err();
            assert_eq!(
                std::mem::discriminant(&error),
                std::mem::discriminant(&expected),
                "{uri}: {error:?}"
            );
            assert!(!error.to_string().contains("secret"));
            assert!(!format!("{error:?}").contains("secret"));
            assert!(error.source().is_none());
        }

        let oversized = "x".repeat(256);
        assert!(matches!(
            Proxy::parse(&format!("socks5h://{oversized}:secret@localhost")),
            Err(InvalidProxy::InvalidUsernameLength)
        ));
        assert!(matches!(
            Proxy::parse(&format!("socks5h://secret:{oversized}@localhost")),
            Err(InvalidProxy::InvalidPasswordLength)
        ));
    }

    /// Parser failures keep their typed causes without retaining the credential-bearing input.
    #[test]
    fn configuration_errors_preserve_parser_sources() {
        for uri in [
            "socks5h://secret:password@localhost:invalid",
            "socks5h://secret:password@localhost:65536",
        ] {
            let error = Proxy::parse(uri).unwrap_err();
            assert!(matches!(
                &error,
                InvalidProxy::InvalidUrl(url::ParseError::InvalidPort)
            ));
            assert_eq!(
                error.source().unwrap().downcast_ref::<url::ParseError>(),
                Some(&url::ParseError::InvalidPort)
            );
            assert!(!error.to_string().contains("secret"));
            assert!(!format!("{error:?}").contains("secret"));
        }
        let error = Proxy::parse("socks5h://secret:%gg@localhost").unwrap_err();
        assert!(matches!(&error, InvalidProxy::InvalidUri(_)));
        assert!(error.source().unwrap().is::<fluent_uri::ParseError>());
        assert!(!error.to_string().contains("secret"));
        assert!(!format!("{error:?}").contains("secret"));
    }

    #[test]
    fn proxy_defaults_and_encoded_credentials_are_preserved() {
        let proxy = Proxy::parse("socks5h://user%40name:pass%3Aword@[::1]").unwrap();
        assert_eq!(proxy.host, "::1");
        assert_eq!(proxy.port, 1080);
        assert!(proxy.remote_dns);
        assert_eq!(
            proxy.credentials,
            Some((b"user@name".to_vec(), b"pass:word".to_vec()))
        );
        let debug = format!("{proxy:?}");
        assert!(!debug.contains("user@name"));
        assert!(!debug.contains("pass:word"));
        assert!(!Proxy::parse("socks5://localhost:9050").unwrap().remote_dns);
    }

    struct Handshake {
        reply: std::io::Cursor<Vec<u8>>,
        sent: Vec<u8>,
    }

    impl std::io::Read for Handshake {
        fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
            self.reply.read(bytes)
        }
    }

    impl std::io::Write for Handshake {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.sent.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn only_a_complete_successful_reply_opens_the_tunnel() {
        use std::io::Read;

        let proxy = Proxy::parse("socks5h://localhost").unwrap();
        for (reply, succeeds) in [
            (vec![5, 0, 5, 0, 0, 1, 127, 0, 0, 1, 0, 80], true),
            (vec![5, 0, 5, 0, 0, 3, 3, b'f', b'o', b'o', 0, 80], true),
            ([vec![5, 0, 5, 0, 0, 4], vec![0; 18]].concat(), true),
            (vec![5, 0, 5, 5, 0, 1], false),
            (vec![5, 0, 4, 0, 0, 1], false),
            (vec![5, 0, 5, 0, 1, 1], false),
            (vec![5, 0, 5, 0, 0, 9], false),
            (vec![5, 0, 5, 0, 0, 1], false),
            (vec![5, 2], false),
            (vec![4, 0], false),
        ] {
            let mut stream = Handshake {
                reply: std::io::Cursor::new(reply),
                sent: Vec::new(),
            };
            assert_eq!(
                proxy.tunnel(&mut stream, "origin.invalid", 80).is_ok(),
                succeeds
            );
            if succeeds {
                let mut remaining = Vec::new();
                stream.read_to_end(&mut remaining).unwrap();
                assert_eq!(remaining, b"");
                assert_eq!(
                    stream.sent,
                    b"\x05\x01\x00\x05\x01\x00\x03\x0eorigin.invalid\x00\x50"
                );
            }
        }
        let mut stream = Handshake {
            reply: std::io::Cursor::new(vec![5, 2, 1, 1]),
            sent: Vec::new(),
        };
        let proxy = Proxy::parse("socks5h://user:pass@localhost").unwrap();
        assert_eq!(
            proxy
                .tunnel(&mut stream, "origin.invalid", 80)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::PermissionDenied
        );
    }
}
