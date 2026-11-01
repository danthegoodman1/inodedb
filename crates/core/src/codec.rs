//! A small binary encoding for records: varints, fixed big-endian integers
//! and length-prefixed bytes.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodeError;

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("malformed record")
    }
}

impl std::error::Error for DecodeError {}

#[derive(Default)]
pub struct Enc {
    pub buf: Vec<u8>,
}

impl Enc {
    pub fn new() -> Enc {
        Enc::default()
    }

    pub fn with_capacity(capacity: usize) -> Enc {
        Enc {
            buf: Vec::with_capacity(capacity),
        }
    }

    pub fn u8(&mut self, value: u8) -> &mut Enc {
        self.buf.push(value);
        self
    }

    pub fn varint(&mut self, mut value: u64) -> &mut Enc {
        while value >= 0x80 {
            self.buf.push((value as u8) | 0x80);
            value >>= 7;
        }
        self.buf.push(value as u8);
        self
    }

    pub fn be64(&mut self, value: u64) -> &mut Enc {
        self.buf.extend_from_slice(&value.to_be_bytes());
        self
    }

    pub fn be16(&mut self, value: u16) -> &mut Enc {
        self.buf.extend_from_slice(&value.to_be_bytes());
        self
    }

    pub fn bytes(&mut self, value: &[u8]) -> &mut Enc {
        self.varint(value.len() as u64);
        self.buf.extend_from_slice(value);
        self
    }

    pub fn raw(&mut self, value: &[u8]) -> &mut Enc {
        self.buf.extend_from_slice(value);
        self
    }

    pub fn finish(self) -> Vec<u8> {
        self.buf
    }
}

pub struct Dec<'a> {
    buf: &'a [u8],
}

impl<'a> Dec<'a> {
    pub fn new(buf: &'a [u8]) -> Dec<'a> {
        Dec { buf }
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn u8(&mut self) -> Result<u8, DecodeError> {
        let (&first, rest) = self.buf.split_first().ok_or(DecodeError)?;
        self.buf = rest;
        Ok(first)
    }

    pub fn varint(&mut self) -> Result<u64, DecodeError> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = self.u8()?;
            value |= u64::from(byte & 0x7f) << shift;
            if byte < 0x80 {
                return Ok(value);
            }
        }
        Err(DecodeError)
    }

    pub fn varint32(&mut self) -> Result<u32, DecodeError> {
        u32::try_from(self.varint()?).map_err(|_| DecodeError)
    }

    pub fn take(&mut self, len: usize) -> Result<&'a [u8], DecodeError> {
        if self.buf.len() < len {
            return Err(DecodeError);
        }
        let (taken, rest) = self.buf.split_at(len);
        self.buf = rest;
        Ok(taken)
    }

    pub fn be64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
        ))
    }

    pub fn be16(&mut self) -> Result<u16, DecodeError> {
        Ok(u16::from_be_bytes(
            self.take(2)?.try_into().expect("2 bytes"),
        ))
    }

    pub fn bytes(&mut self) -> Result<&'a [u8], DecodeError> {
        let len = usize::try_from(self.varint()?).map_err(|_| DecodeError)?;
        self.take(len)
    }

    pub fn rest(&mut self) -> &'a [u8] {
        std::mem::take(&mut self.buf)
    }

    pub fn end(&self) -> Result<(), DecodeError> {
        if self.buf.is_empty() {
            Ok(())
        } else {
            Err(DecodeError)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_round_trip() {
        let mut enc = Enc::new();
        enc.varint(0)
            .varint(127)
            .varint(128)
            .varint(u64::MAX)
            .be64(7)
            .bytes(b"name");
        let buf = enc.finish();
        let mut dec = Dec::new(&buf);
        assert_eq!(dec.varint(), Ok(0));
        assert_eq!(dec.varint(), Ok(127));
        assert_eq!(dec.varint(), Ok(128));
        assert_eq!(dec.varint(), Ok(u64::MAX));
        assert_eq!(dec.be64(), Ok(7));
        assert_eq!(dec.bytes(), Ok(&b"name"[..]));
        assert_eq!(dec.end(), Ok(()));
    }

    #[test]
    fn truncated_input_fails() {
        let mut dec = Dec::new(&[0x80]);
        assert_eq!(dec.varint(), Err(DecodeError));
        let mut dec = Dec::new(&[5, b'a']);
        assert_eq!(dec.bytes(), Err(DecodeError));
    }
}
