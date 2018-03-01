extern crate sled;
extern crate libc;

use std::ffi::CString;
use std::mem;
use std::ptr;
use std::slice;

use libc::*;

use sled::{ConfigBuilder, Error, Iter, Tree};

fn leak_buf(v: Vec<u8>, vallen: *mut size_t) -> *mut c_char {
    unsafe {
        *vallen = v.len();
    }
    let mut bsv = v.into_boxed_slice();
    let val = bsv.as_mut_ptr() as *mut i8;
    mem::forget(bsv);
    val
}

/// Create a new configuration.
#[no_mangle]
pub unsafe extern "C" fn sled_create_config() -> *mut ConfigBuilder {
    let ptr = Box::into_raw(Box::new(ConfigBuilder::new()));
    ptr
}

/// Destroy a configuration.
#[no_mangle]
pub unsafe extern "C" fn sled_free_config(config: *mut ConfigBuilder) {
    drop(Box::from_raw(config));
}

/// Set the configured file path. The caller is responsible for freeing the path string after
/// calling this (it is copied in this function).
#[no_mangle]
pub unsafe extern "C" fn sled_config_set_path(
    config: *mut ConfigBuilder,
    path: *const c_char,
) {
    let c_str = CString::from_raw(path as *mut i8);
    let value = c_str.into_string().unwrap();

    (*config).set_path(value)
}

/// Configure read-only mode.
#[no_mangle]
pub unsafe extern "C" fn sled_config_read_only(
    config: *mut ConfigBuilder,
    read_only: c_uchar,
) {
    (*config).set_read_only(read_only == 1)
}

/// Set the configured cache capacity in bytes.
#[no_mangle]
pub unsafe extern "C" fn sled_config_set_cache_capacity(
    config: *mut ConfigBuilder,
    capacity: size_t,
) {
    (*config).set_cache_capacity(capacity)
}

/// Configure the use of the zstd compression library.
#[no_mangle]
pub unsafe extern "C" fn sled_config_use_compression(
    config: *mut ConfigBuilder,
    use_compression: c_uchar,
) {
    (*config).set_use_compression(use_compression == 1)
}

/// Set the configured IO buffer flush interval in milliseconds.
#[no_mangle]
pub unsafe extern "C" fn sled_config_flush_every_ms(
    config: *mut ConfigBuilder,
    flush_every: c_int,
) {
    let val = if flush_every < 0 {
        None
    } else {
        Some(flush_every as u64)
    };
    (*config).set_flush_every_ms(val)
}

/// Set the configured snapshot operation threshold.
#[no_mangle]
pub unsafe extern "C" fn sled_config_snapshot_after_ops(
    config: *mut ConfigBuilder,
    snapshot_after: size_t,
) {
    (*config).set_snapshot_after_ops(snapshot_after)
}

/// Open a sled lock-free log-structured tree. Consumes the passed-in config.
#[no_mangle]
pub unsafe extern "C" fn sled_open_tree(
    config: *mut ConfigBuilder,
) -> *mut Tree {
    let conf_2 = (*config).clone();
    let conf_3 = conf_2.build();
    sled_free_config(config);
    Box::into_raw(Box::new(Tree::start(conf_3).unwrap()))
}

/// Close a sled lock-free log-structured tree.
#[no_mangle]
pub unsafe extern "C" fn sled_close(db: *mut Tree) {
    drop(Box::from_raw(db));
}

/// Free a buffer originally allocated by sled.
#[no_mangle]
pub unsafe extern "C" fn sled_free_buf(buf: *mut c_char, sz: size_t) {
    drop(Vec::from_raw_parts(buf, sz, sz));
}

/// Free a Tree created by sled.
#[no_mangle]
pub unsafe extern "C" fn sled_free_tree(tree: *mut Tree) {
    drop(Box::from_raw(tree));
}

/// Free an iterator.
#[no_mangle]
pub unsafe extern "C" fn sled_free_iter(iter: *mut Iter) {
    drop(Box::from_raw(iter));
}

/// Set a key to a value.
#[no_mangle]
pub unsafe extern "C" fn sled_set(
    db: *mut Tree,
    key: *const c_uchar,
    keylen: size_t,
    val: *const c_uchar,
    vallen: size_t,
) {
    let k = slice::from_raw_parts(key, keylen).to_vec();
    let v = slice::from_raw_parts(val, vallen).to_vec();
    (*db).set(k.clone(), v.clone()).unwrap();
}

/// Get the value of a key.
/// Caller is responsible for freeing the returned value with `sled_free_buf` if it's non-null.
#[no_mangle]
pub unsafe extern "C" fn sled_get(
    db: *mut Tree,
    key: *const c_char,
    keylen: size_t,
    vallen: *mut size_t,
) -> *mut c_char {
    let k = slice::from_raw_parts(key as *const u8, keylen);
    let res = (*db).get(k);
    match res {
        Ok(Some(v)) => leak_buf(v, vallen),
        Ok(None) => ptr::null_mut(),
        // TODO proper error propagation
        Err(e) => panic!("{:?}", e),
    }
}

/// Delete the value of a key.
#[no_mangle]
pub unsafe extern "C" fn sled_del(
    db: *mut Tree,
    key: *const c_char,
    keylen: size_t,
) {
    let k = slice::from_raw_parts(key as *const u8, keylen);
    (*db).del(k).unwrap();
}

/// Compare and swap.
/// Returns 1 if successful, 0 if unsuccessful.
/// Otherwise sets `actual_val` and `actual_vallen` to the current value,
/// which must be freed using `sled_free_buf` by the caller if non-null.
/// `actual_val` will be null and `actual_vallen` 0 if the current value is not set.
#[no_mangle]
pub unsafe extern "C" fn sled_cas(
    db: *mut Tree,
    key: *const c_char,
    keylen: size_t,
    old_val: *const c_uchar,
    old_vallen: size_t,
    new_val: *const c_uchar,
    new_vallen: size_t,
    actual_val: *mut *const c_uchar,
    actual_vallen: *mut size_t,
) -> c_uchar {
    let k = slice::from_raw_parts(key as *const u8, keylen).to_vec();

    let old = if old_vallen == 0 {
        None
    } else {
        let old_slice = slice::from_raw_parts(old_val as *const u8, old_vallen);
        let copy = old_slice.to_vec();
        Some(copy)
    };

    let new = if new_vallen == 0 {
        None
    } else {
        let new_slice = slice::from_raw_parts(new_val as *const u8, new_vallen);
        let copy = new_slice.to_vec();
        Some(copy)
    };

    let res = (*db).cas(k.clone(), old, new);

    match res {
        Ok(()) => {
            1
        }
        Err(Error::CasFailed(None)) => {
            *actual_vallen = 0;
            0
        }
        Err(Error::CasFailed(Some(v))) => {
            *actual_val = leak_buf(v, actual_vallen) as *const u8;
            0
        }
        // TODO proper error propagation
        Err(e) => panic!("{:?}", e),
    }
}

/// Iterate from a starting key.
/// Caller is responsible for freeing the returned iterator with `sled_free_iter`.
#[no_mangle]
pub unsafe extern "C" fn sled_scan<'a>(
    db: *mut Tree,
    key: *const c_char,
    keylen: size_t,
) -> *mut Iter<'a> {
    let k = slice::from_raw_parts(key as *const u8, keylen);
    Box::into_raw(Box::new((*db).scan(k)))
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
            *key = leak_buf(k, keylen);
            *val = leak_buf(v, vallen);
            1
        }
        // TODO proper error propagation
        Some(Err(e)) => panic!("{:?}", e),
        None => 0,
    }
}
