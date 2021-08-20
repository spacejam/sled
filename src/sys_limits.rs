#![allow(unsafe_code)]

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::io;
#[cfg(any(target_os = "linux"))]
use {std::fs::File, std::io::Read};

use std::convert::TryFrom;

/// See the Kernel's documentation for more information about this subsystem,
/// found at:  [Documentation/cgroup-v1/memory.txt](https://www.kernel.org/doc/Documentation/cgroup-v1/memory.txt)
///
/// If there's no memory limit specified on the container this may return
/// 0x7FFFFFFFFFFFF000 (2^63-1 rounded down to 4k which is a common page size).
/// So we know we are not running in a memory restricted environment.
#[cfg(target_os = "linux")]
fn get_cgroup_memory_limit() -> io::Result<u64> {
    File::open("/sys/fs/cgroup/memory/memory.limit_in_bytes")
        .and_then(read_u64_from)
}

#[cfg(target_os = "linux")]
fn read_u64_from(mut file: File) -> io::Result<u64> {
    let mut s = String::new();
    file.read_to_string(&mut s).and_then(|_| {
        s.trim()
            .parse()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    })
}

/// Returns the maximum size of total available memory of the process, in bytes.
/// If this limit is exceeded, the malloc() and mmap() functions shall fail with
/// errno set to [ENOMEM].
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn get_rlimit_as() -> io::Result<libc::rlimit> {
    let mut limit = std::mem::MaybeUninit::<libc::rlimit>::uninit();

    let ret = unsafe { libc::getrlimit(libc::RLIMIT_AS, limit.as_mut_ptr()) };

    if ret == 0 {
        Ok(unsafe { limit.assume_init() })
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub fn get_available_memory() -> io::Result<usize> {
    let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
    if pages == -1 {
        return Err(io::Error::last_os_error());
    }

    let page_size = unsafe { libc::sysconf(libc::_SC_PAGE_SIZE) };
    if page_size == -1 {
        return Err(io::Error::last_os_error());
    }

    Ok(usize::try_from(pages).unwrap() * usize::try_from(page_size).unwrap())
}

#[cfg(miri)]
pub fn get_memory_limit() -> Option<usize> {
    None
}

#[cfg(not(miri))]
pub fn get_memory_limit() -> Option<usize> {
    let mut max: u64 = 0;

    #[cfg(target_os = "linux")]
    {
        if let Ok(mem) = get_cgroup_memory_limit() {
            max = mem;
        }

        // If there's no memory limit specified on the container this
        // actually returns 0x7FFFFFFFFFFFF000 (2^63-1 rounded down to
        // 4k which is a common page size). So we know we are not
        // running in a memory restricted environment.
        // src: https://github.com/dotnet/coreclr/blob/master/src/pal/src/misc/cgroup.cpp#L385-L428
        if max > 0x7FFF_FFFF_0000_0000 {
            return None;
        }
    }

    #[allow(clippy::useless_conversion)]
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        if let Ok(rlim) = get_rlimit_as() {
            let rlim_cur = u64::try_from(rlim.rlim_cur).unwrap();
            if max == 0 || rlim_cur < max {
                max = rlim_cur;
            }
        }

        if let Ok(available) = get_available_memory() {
            if max == 0 || (available as u64) < max {
                max = available as u64;
            }
        }
    }

    #[cfg(miri)]
    {
        // Miri has a significant memory consumption overhead. During a small
        // test run, a memory amplification of ~35x was observed. Certain
        // memory overheads may increase asymptotically with longer test runs,
        // such as the interpreter's dead_alloc_map. Memory overhead is
        // dominated by stacked borrows tags; the asymptotic behavior of this
        // overhead needs further investigation.
        max /= 40;
    }

    let ret = usize::try_from(max).expect("cache limit not representable");
    if ret == 0 { None } else { Some(ret) }
}
