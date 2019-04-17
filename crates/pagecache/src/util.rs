use std::convert::TryInto;

#[inline]
pub(crate) fn u64_to_arr(number: u64) -> [u8; 8] {
    number.to_le_bytes()
}

#[inline]
pub(crate) fn arr_to_u64(arr: &[u8]) -> u64 {
    arr.try_into().map(u64::from_le_bytes).unwrap()
}

#[inline]
pub(crate) fn arr_to_u32(arr: &[u8]) -> u32 {
    arr.try_into().map(u32::from_le_bytes).unwrap()
}

#[inline]
pub(crate) fn u32_to_arr(number: u32) -> [u8; 4] {
    number.to_le_bytes()
}
