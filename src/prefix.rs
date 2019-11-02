use super::*;

pub(crate) fn empty() -> &'static [u8] {
    &[]
}

pub(crate) fn reencode(
    old_prefix: &[u8],
    old_encoded_key: &IVec,
    new_prefix_length: usize,
) -> IVec {
    let new_encoded_key: Vec<u8> = old_prefix
        .iter()
        .chain(old_encoded_key.iter())
        .skip(new_prefix_length)
        .copied()
        .collect();

    IVec::from(new_encoded_key)
}

pub(crate) fn decode(old_prefix: &[u8], old_encoded_key: &[u8]) -> IVec {
    let mut decoded_key =
        Vec::with_capacity(old_prefix.len() + old_encoded_key.len());
    decoded_key.extend_from_slice(old_prefix);
    decoded_key.extend_from_slice(old_encoded_key);

    IVec::from(decoded_key)
}
