#![allow(dead_code)]

mod hash;
mod crc16;
mod crc64;

// used for fast lookups
pub use self::hash::hash;

// used for protecting large snapshot files
pub use self::crc64::crc64;

// used for protecting individual log entries
pub use self::crc16::{crc16, crc16_arr};
