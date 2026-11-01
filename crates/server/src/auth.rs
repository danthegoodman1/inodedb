//! Keys as requests present them, and handles bound to the key that
//! received them.
//!
//! A key is `<id>.<secret>`: the id in 16 hex digits, the secret 16
//! characters or more. The writer holds each key's record and checks the
//! SHA-256 of the presented secret against it. A handle carries a MAC over
//! the key's id and the inode, under a secret kept in storage, so it works
//! only with the key it was issued to.

use hmac::{Hmac, KeyInit, Mac};
use inodedb_core::auth::{Caller, Denied, KeyId};
use inodedb_core::types::Ino;
use inodedb_proto as pb;
use sha2::{Digest, Sha256};
use tonic::{Request, Status};

pub use inodedb_core::auth::Grant;

/// The shortest secret a key may have.
pub const MIN_SECRET: usize = 16;
/// A handle's MAC length.
const MAC_BYTES: usize = 16;

/// A key's id and secret, if `key` has the form `<id>.<secret>`.
pub fn parse(key: &str) -> Option<(KeyId, &str)> {
    let (id, secret) = key.split_once('.')?;
    if id.len() != 16 || secret.len() < MIN_SECRET {
        return None;
    }
    Some((KeyId::from_str_radix(id, 16).ok()?, secret))
}

pub fn hash(secret: &str) -> [u8; 32] {
    Sha256::digest(secret.as_bytes()).into()
}

pub fn format(id: KeyId, secret: &str) -> String {
    format!("{id:016x}.{secret}")
}

/// A random key: an id and a 128-bit secret.
pub fn generate() -> (KeyId, String) {
    let id = getrandom::u64().expect("the system's random source");
    let mut secret = [0u8; 16];
    getrandom::fill(&mut secret).expect("the system's random source");
    let secret = secret.iter().map(|byte| format!("{byte:02x}")).collect();
    (id, secret)
}

pub fn denied(denied: Denied) -> Status {
    match denied {
        Denied::Unknown => Status::unauthenticated("unknown or revoked key"),
        Denied::Expired => Status::unauthenticated("the key expired"),
        Denied::Grant => Status::permission_denied("the key lacks the grant"),
    }
}

/// Reads requests' keys, under the store's handle secret.
pub struct Auth {
    handles: Hmac<Sha256>,
}

impl Auth {
    pub fn new(handle_secret: [u8; 32]) -> Auth {
        Auth {
            handles: Hmac::new_from_slice(&handle_secret).expect("HMAC takes any key length"),
        }
    }

    /// The caller a request's key names, and the scope its handles use.
    pub fn caller<T>(&self, request: &Request<T>) -> Result<(Caller, Scope), Status> {
        let presented = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .ok_or_else(|| Status::unauthenticated("no key"))?;
        let (id, secret) = parse(presented)
            .ok_or_else(|| Status::unauthenticated("a key is <16 hex digits>.<secret>"))?;
        let mut handles = self.handles.clone();
        handles.update(&id.to_be_bytes());
        Ok((
            Caller::Key {
                id,
                hash: hash(secret),
            },
            Scope { handles },
        ))
    }
}

/// Makes and checks one key's handles.
#[derive(Clone)]
pub struct Scope {
    /// The MAC, already fed the key's id.
    handles: Hmac<Sha256>,
}

impl Scope {
    fn mac(&self, ino: Ino) -> [u8; MAC_BYTES] {
        let mut mac = self.handles.clone();
        mac.update(&ino.to_be_bytes());
        let full = mac.finalize().into_bytes();
        let mut short = [0; MAC_BYTES];
        short.copy_from_slice(&full[..MAC_BYTES]);
        short
    }

    pub fn handle(&self, ino: Ino) -> pb::Handle {
        pb::Handle {
            ino,
            mac: self.mac(ino).to_vec().into(),
        }
    }

    /// The inode a handle names, if this key received it.
    pub fn check(&self, handle: &pb::Handle) -> Result<Ino, Status> {
        let expected = self.mac(handle.ino);
        let given: &[u8] = &handle.mac;
        let same = given.len() == MAC_BYTES
            && given
                .iter()
                .zip(expected)
                .fold(0u8, |diff, (x, y)| diff | (x ^ y))
                == 0;
        if same {
            Ok(handle.ino)
        } else {
            Err(Status::invalid_argument(
                "the handle belongs to another key",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_parse() {
        let key = format(0xab, "0123456789abcdef");
        assert_eq!(key, "00000000000000ab.0123456789abcdef");
        assert_eq!(parse(&key), Some((0xab, "0123456789abcdef")));
        for bad in [
            "ab.0123456789abcdef",
            "00000000000000ab.short",
            "nodot",
            "zzzzzzzzzzzzzzzz.0123456789abcdef",
        ] {
            assert_eq!(parse(bad), None, "{bad}");
        }
        let (id, secret) = generate();
        assert_eq!(parse(&format(id, &secret)), Some((id, secret.as_str())));
    }

    #[test]
    fn a_handle_works_only_with_its_key() {
        let auth = Auth::new([3; 32]);
        let scope = |key: &str| {
            let mut request = Request::new(());
            request
                .metadata_mut()
                .insert("authorization", format!("Bearer {key}").parse().unwrap());
            auth.caller(&request).unwrap().1
        };
        let mine = scope("0000000000000001.0123456789abcdef");
        let theirs = scope("0000000000000002.0123456789abcdef");
        let handle = mine.handle(42);
        assert_eq!(mine.check(&handle).ok(), Some(42));
        assert!(theirs.check(&handle).is_err());
        let forged = pb::Handle {
            ino: 43,
            mac: handle.mac.clone(),
        };
        assert!(mine.check(&forged).is_err());
    }
}
