//! this example demonstrates how to work with structured
//! keys and values without paying expensive (de)serialization
//! costs.
use {
    byteorder::{BigEndian, LittleEndian},
    zerocopy::{byteorder::U64, AsBytes, FromBytes, LayoutVerified, Unaligned},
};

// we use `BigEndian` for key types because
// they preserve lexicographic ordering,
// which is nice if we ever want to iterate
// over our items in order.
#[derive(FromBytes, AsBytes, Unaligned)]
#[repr(C)]
struct Key {
    a: U64<BigEndian>,
    b: U64<BigEndian>,
}

// we use `LittleEndian` for values because
// it's possibly cheaper, but the difference
// isn't likely to be measurable, so honestly
// use whatever you want for values.
#[derive(FromBytes, AsBytes, Unaligned)]
#[repr(C)]
struct Value {
    count: U64<LittleEndian>,
    whatever: [u8; 16],
}

fn main() -> sled::Result<()> {
    let db = sled::open("my_database")?;

    let key = Key { a: U64::new(21), b: U64::new(890) };

    // "UPSERT" functionality
    db.update_and_fetch(key.as_bytes(), |value_opt| {
        if let Some(existing) = value_opt {
            // IVec will be stack-allocated until it reaches 22 bytes
            let mut backing_bytes = sled::IVec::from(existing);

            // this verifies that our value is the correct length
            // and alignment (in this case we don't need it to be
            // aligned, because we use the `U64` type from zerocopy)
            let layout: LayoutVerified<&mut [u8], Value> =
                LayoutVerified::new_unaligned(&mut *backing_bytes)
                    .expect("bytes do not fit schema");

            // this lets us work with the underlying bytes as
            // a mutable structured value.
            let value: &mut Value = layout.into_mut();

            let new_count = value.count.get() + 1;

            println!("incrementing count to {}", new_count);

            value.count.set(new_count);

            Some(backing_bytes)
        } else {
            println!("setting count to 0");

            Some(sled::IVec::from(
                Value { count: U64::new(0), whatever: [0; 16] }.as_bytes(),
            ))
        }
    })?;

    Ok(())
}
