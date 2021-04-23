use std::convert::TryFrom;

/// Returns the number of bytes that this varint will need
pub const fn size(int: u64) -> usize {
    if int <= 240 {
        1
    } else if int <= 2287 {
        2
    } else if int <= 67823 {
        3
    } else if int <= 0x00FF_FFFF {
        4
    } else if int <= 0xFFFF_FFFF {
        5
    } else if int <= 0x00FF_FFFF_FFFF {
        6
    } else if int <= 0xFFFF_FFFF_FFFF {
        7
    } else if int <= 0x00FF_FFFF_FFFF_FFFF {
        8
    } else {
        9
    }
}

/// Returns how many bytes the varint consumed while serializing
pub fn serialize_into(int: u64, buf: &mut [u8]) -> usize {
    if int <= 240 {
        buf[0] = u8::try_from(int).unwrap();
        1
    } else if int <= 2287 {
        buf[0] = u8::try_from((int - 240) / 256 + 241).unwrap();
        buf[1] = u8::try_from((int - 240) % 256).unwrap();
        2
    } else if int <= 67823 {
        buf[0] = 249;
        buf[1] = u8::try_from((int - 2288) / 256).unwrap();
        buf[2] = u8::try_from((int - 2288) % 256).unwrap();
        3
    } else if int <= 0x00FF_FFFF {
        buf[0] = 250;
        let bytes = int.to_le_bytes();
        buf[1..4].copy_from_slice(&bytes[..3]);
        4
    } else if int <= 0xFFFF_FFFF {
        buf[0] = 251;
        let bytes = int.to_le_bytes();
        buf[1..5].copy_from_slice(&bytes[..4]);
        5
    } else if int <= 0x00FF_FFFF_FFFF {
        buf[0] = 252;
        let bytes = int.to_le_bytes();
        buf[1..6].copy_from_slice(&bytes[..5]);
        6
    } else if int <= 0xFFFF_FFFF_FFFF {
        buf[0] = 253;
        let bytes = int.to_le_bytes();
        buf[1..7].copy_from_slice(&bytes[..6]);
        7
    } else if int <= 0x00FF_FFFF_FFFF_FFFF {
        buf[0] = 254;
        let bytes = int.to_le_bytes();
        buf[1..8].copy_from_slice(&bytes[..7]);
        8
    } else {
        buf[0] = 255;
        let bytes = int.to_le_bytes();
        buf[1..9].copy_from_slice(&bytes[..8]);
        9
    }
}

/// Returns the deserialized varint, along with how many bytes
/// were taken up by the varint.
pub fn deserialize(buf: &[u8]) -> crate::Result<(u64, usize)> {
    if buf.is_empty() {
        return Err(crate::Error::corruption(None));
    }
    let res = match buf[0] {
        0..=240 => (u64::from(buf[0]), 1),
        241..=248 => {
            let varint =
                240 + 256 * (u64::from(buf[0]) - 241) + u64::from(buf[1]);
            (varint, 2)
        }
        249 => {
            let varint = 2288 + 256 * u64::from(buf[1]) + u64::from(buf[2]);
            (varint, 3)
        }
        other => {
            let sz = other as usize - 247;
            let mut aligned = [0; 8];
            aligned[..sz].copy_from_slice(&buf[1..=sz]);
            let varint = u64::from_le_bytes(aligned);
            (varint, sz + 1)
        }
    };
    Ok(res)
}
