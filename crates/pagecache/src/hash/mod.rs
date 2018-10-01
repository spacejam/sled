mod crc16;
mod crc64;

// used for protecting large snapshot files
pub(crate) use self::crc64::crc64;

// used for protecting individual log entries
pub(crate) use self::crc16::crc16_arr;
