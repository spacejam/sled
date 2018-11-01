pub(crate) fn u64_to_arr(u: u64) -> [u8; 8] {
    [
        u as u8,
        (u >> 8) as u8,
        (u >> 16) as u8,
        (u >> 24) as u8,
        (u >> 32) as u8,
        (u >> 40) as u8,
        (u >> 48) as u8,
        (u >> 56) as u8,
    ]
}

pub(crate) fn arr_to_u64(arr: [u8; 8]) -> u64 {
    arr[0] as u64
        + ((arr[1] as u64) << 8)
        + ((arr[2] as u64) << 16)
        + ((arr[3] as u64) << 24)
        + ((arr[4] as u64) << 32)
        + ((arr[5] as u64) << 40)
        + ((arr[6] as u64) << 48)
        + ((arr[7] as u64) << 56)
}

pub(crate) fn arr_to_u32(arr: [u8; 4]) -> u32 {
    arr[0] as u32
        + ((arr[1] as u32) << 8)
        + ((arr[2] as u32) << 16)
        + ((arr[3] as u32) << 24)
}

pub(crate) fn u32_to_arr(u: u32) -> [u8; 4] {
    [u as u8, (u >> 8) as u8, (u >> 16) as u8, (u >> 24) as u8]
}
