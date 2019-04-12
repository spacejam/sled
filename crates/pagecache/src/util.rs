#[inline]
pub(crate) fn u64_to_arr(number: u64) -> [u8; 8] {
    number.to_le_bytes()
}

#[inline]
pub(crate) fn arr_to_u64(arr: &[u8]) -> u64 {
    debug_assert_eq!(arr.len(), 8);
    u64::from(arr[0])
        + ((u64::from(arr[1])) << 8)
        + ((u64::from(arr[2])) << 16)
        + ((u64::from(arr[3])) << 24)
        + ((u64::from(arr[4])) << 32)
        + ((u64::from(arr[5])) << 40)
        + ((u64::from(arr[6])) << 48)
        + ((u64::from(arr[7])) << 56)
}

#[inline]
pub(crate) fn arr_to_u32(arr: &[u8]) -> u32 {
    debug_assert_eq!(arr.len(), 4);
    u32::from(arr[0])
        + ((u32::from(arr[1])) << 8)
        + ((u32::from(arr[2])) << 16)
        + ((u32::from(arr[3])) << 24)
}

#[inline]
pub(crate) fn u32_to_arr(number: u32) -> [u8; 4] {
    number.to_le_bytes()
}
