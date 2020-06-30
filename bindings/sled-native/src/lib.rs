use sled;

use std::ffi::CString;
use std::mem;
use std::ptr;
use std::slice;

use libc::*;

use sled::{Config, Db, IVec, Iter};

fn leak_buf(v: Vec<u8>, vallen: *mut size_t) -> *mut c_char {
    unsafe {
        *vallen = v.len();
    }
    let mut bsv = v.into_boxed_slice();
    let val = bsv.as_mut_ptr() as *mut _;
    mem::forget(bsv);
    val
}

/// Create a new configuration.
#[no_mangle]
pub unsafe extern "C" fn sled_create_config() -> *mut Config {
    Box::into_raw(Box::new(Config::new()))
}

/// Destroy a configuration.
#[no_mangle]
pub unsafe extern "C" fn sled_free_config(config: *mut Config) {
    drop(Box::from_raw(config));
}

/// Set the configured file path. The caller is responsible for freeing the path
/// string after calling this (it is copied in this function).
#[no_mangle]
pub unsafe extern "C" fn sled_config_set_path(
    config: *mut Config,
    path: *const c_char,
) -> *mut Config {
    let c_str = CString::from_raw(path as *mut _);
    let value = c_str.into_string().unwrap();

    let config = Box::from_raw(config);
    Box::into_raw(Box::from(config.path(value)))
}

/// Set the configured cache capacity in bytes.
#[no_mangle]
pub unsafe extern "C" fn sled_config_set_cache_capacity(
    config: *mut Config,
    capacity: size_t,
) -> *mut Config {
    let config = Box::from_raw(config);
    Box::into_raw(Box::from(config.cache_capacity(capacity as u64)))
}

/// Configure the use of the zstd compression library.
#[no_mangle]
pub unsafe extern "C" fn sled_config_use_compression(
    config: *mut Config,
    use_compression: c_uchar,
) -> *mut Config {
    let config = Box::from_raw(config);
    Box::into_raw(Box::from(config.use_compression(use_compression == 1)))
}

/// Set the configured IO buffer flush interval in milliseconds.
#[no_mangle]
pub unsafe extern "C" fn sled_config_flush_every_ms(
    config: *mut Config,
    flush_every: c_int,
) -> *mut Config {
    let val = if flush_every < 0 { None } else { Some(flush_every as u64) };
    let config = Box::from_raw(config);
    Box::into_raw(Box::from(config.flush_every_ms(val)))
}

/// Open a sled lock-free log-structured tree. Consumes the passed-in config.
#[no_mangle]
pub unsafe extern "C" fn sled_open_db(config: *mut Config) -> *mut Db {
    let config = Box::from_raw(config);
    Box::into_raw(Box::new(config.open().unwrap()))
}

/// Close a sled lock-free log-structured tree.
#[no_mangle]
pub unsafe extern "C" fn sled_close(db: *mut Db) {
    drop(Box::from_raw(db));
}

/// Free a buffer originally allocated by sled.
#[no_mangle]
pub unsafe extern "C" fn sled_free_buf(buf: *mut c_char, sz: size_t) {
    drop(Vec::from_raw_parts(buf, sz, sz));
}

/// Free an iterator.
#[no_mangle]
pub unsafe extern "C" fn sled_free_iter(iter: *mut Iter) {
    drop(Box::from_raw(iter));
}

/// Set a key to a value.
#[no_mangle]
pub unsafe extern "C" fn sled_set(
    db: *mut Db,
    key: *const c_uchar,
    keylen: size_t,
    val: *const c_uchar,
    vallen: size_t,
) {
    let k = IVec::from(slice::from_raw_parts(key, keylen));
    let v = IVec::from(slice::from_raw_parts(val, vallen));
    (*db).insert(k, v).unwrap();
}

/// Get the value of a key.
/// Caller is responsible for freeing the returned value with `sled_free_buf` if
/// it's non-null.
#[no_mangle]
pub unsafe extern "C" fn sled_get(
    db: *mut Db,
    key: *const c_char,
    keylen: size_t,
    vallen: *mut size_t,
) -> *mut c_char {
    let k = slice::from_raw_parts(key as *const u8, keylen);
    let res = (*db).get(k);
    match res {
        Ok(Some(v)) => leak_buf(v.to_vec(), vallen),
        Ok(None) => ptr::null_mut(),
        // TODO proper error propagation
        Err(e) => panic!("{:?}", e),
    }
}

/// Delete the value of a key.
#[no_mangle]
pub unsafe extern "C" fn sled_del(
    db: *mut Db,
    key: *const c_char,
    keylen: size_t,
) {
    let k = slice::from_raw_parts(key as *const u8, keylen);
    (*db).remove(k).unwrap();
}

/// Compare and swap.
/// Returns 1 if successful, 0 if unsuccessful.
/// Otherwise sets `actual_val` and `actual_vallen` to the current value,
/// which must be freed using `sled_free_buf` by the caller if non-null.
/// `actual_val` will be null and `actual_vallen` 0 if the current value is not
/// set.
#[no_mangle]
pub unsafe extern "C" fn sled_compare_and_swap(
    db: *mut Db,
    key: *const c_char,
    keylen: size_t,
    old_val: *const c_uchar,
    old_vallen: size_t,
    new_val: *const c_uchar,
    new_vallen: size_t,
    actual_val: *mut *const c_uchar,
    actual_vallen: *mut size_t,
) -> c_uchar {
    let k = IVec::from(slice::from_raw_parts(key as *const u8, keylen));

    let old = if old_vallen == 0 {
        None
    } else {
        let copy =
            IVec::from(slice::from_raw_parts(old_val as *const u8, old_vallen));
        Some(copy)
    };

    let new = if new_vallen == 0 {
        None
    } else {
        let copy =
            IVec::from(slice::from_raw_parts(new_val as *const u8, new_vallen));
        Some(copy)
    };

    let res = (*db).compare_and_swap(k, old, new);

    match res {
        Ok(Ok(())) => 1,
        Ok(Err(sled::CompareAndSwapError { current: None, .. })) => {
            *actual_vallen = 0;
            0
        }
        Ok(Err(sled::CompareAndSwapError { current: Some(v), .. })) => {
            *actual_val = leak_buf(v.to_vec(), actual_vallen) as *const u8;
            0
        }
        // TODO proper error propagation
        Err(e) => panic!("{:?}", e),
    }
}

/// Iterate over tuples which have specified key prefix.
/// Caller is responsible for freeing the returned iterator with
/// `sled_free_iter`.
#[no_mangle]
pub unsafe extern "C" fn sled_scan_prefix(
    db: *mut Db,
    key: *const c_char,
    keylen: size_t,
) -> *mut Iter {
    let k = slice::from_raw_parts(key as *const u8, keylen);
    Box::into_raw(Box::new((*db).scan_prefix(k)))
}

/// Get they next kv pair from an iterator.
/// Caller is responsible for freeing the key and value with `sled_free_buf`.
/// Returns 0 when exhausted.
#[no_mangle]
pub unsafe extern "C" fn sled_iter_next(
    iter: *mut Iter,
    key: *mut *const c_char,
    keylen: *mut size_t,
    val: *mut *const c_char,
    vallen: *mut size_t,
) -> c_uchar {
    match (*iter).next() {
        Some(Ok((k, v))) => {
            *key = leak_buf(k.to_vec(), keylen);
            *val = leak_buf(v.to_vec(), vallen);
            1
        }
        // TODO proper error propagation
        Some(Err(e)) => panic!("{:?}", e),
        None => 0,
    }
}
