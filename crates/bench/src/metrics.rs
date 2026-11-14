//! Process-level measurements: CPU time, peak RSS, allocation counters.
//!
//! Unix uses `getrusage`; Windows uses `GetProcessTimes` / `GetProcessMemoryInfo`.

use std::time::Duration;

use crate::alloc_counter::{self, AllocStats};

/// Cross-platform access to this process's CPU time and memory high-water mark.
pub struct ProcessMetrics;

/// A snapshot of process resource counters.
#[derive(Clone, Copy, Debug)]
pub struct Snapshot {
    pub user: Duration,
    pub sys: Duration,
    pub alloc: AllocStats,
}

impl Snapshot {
    /// Samples user/sys CPU (all threads) and allocator counters now.
    pub fn take() -> Self {
        let (user, sys) = ProcessMetrics::cpu_times();
        Self {
            user,
            sys,
            alloc: alloc_counter::stats(),
        }
    }
}

/// Peak resident set size of the process so far, in bytes.
pub fn peak_rss_bytes() -> u64 {
    ProcessMetrics::peak_rss_bytes()
}

#[cfg(unix)]
impl ProcessMetrics {
    fn rusage() -> libc::rusage {
        // SAFETY: getrusage fully initialises the struct on success; zeroed is a valid fallback.
        unsafe {
            let mut ru: libc::rusage = std::mem::zeroed();
            libc::getrusage(libc::RUSAGE_SELF, &mut ru);
            ru
        }
    }

    /// Cumulative (user, system) CPU time of all threads.
    pub fn cpu_times() -> (Duration, Duration) {
        let tv = |t: libc::timeval| Duration::new(t.tv_sec as u64, t.tv_usec as u32 * 1000);
        let ru = Self::rusage();
        (tv(ru.ru_utime), tv(ru.ru_stime))
    }

    /// Peak RSS in bytes. `ru_maxrss` is bytes on macOS and KiB elsewhere. It is a
    /// process-lifetime high-water mark (includes warmup and earlier runs).
    pub fn peak_rss_bytes() -> u64 {
        let v = Self::rusage().ru_maxrss.max(0) as u64;
        if cfg!(target_os = "macos") {
            v
        } else {
            v * 1024
        }
    }
}

#[cfg(windows)]
impl ProcessMetrics {
    /// Cumulative (user, kernel) CPU time of all threads.
    pub fn cpu_times() -> (Duration, Duration) {
        use windows_sys::Win32::{
            Foundation::FILETIME,
            System::Threading::{GetCurrentProcess, GetProcessTimes},
        };
        let ft = |f: FILETIME| {
            Duration::from_nanos((((f.dwHighDateTime as u64) << 32) | f.dwLowDateTime as u64) * 100)
        };
        let zero = FILETIME {
            dwLowDateTime: 0,
            dwHighDateTime: 0,
        };
        let (mut create, mut exit, mut kernel, mut user) = (zero, zero, zero, zero);
        // SAFETY: all out-pointers are valid for the duration of the call.
        unsafe {
            GetProcessTimes(
                GetCurrentProcess(),
                &mut create,
                &mut exit,
                &mut kernel,
                &mut user,
            )
        };
        (ft(user), ft(kernel))
    }

    /// Peak working set in bytes (process-lifetime high-water mark).
    pub fn peak_rss_bytes() -> u64 {
        use windows_sys::Win32::System::{
            ProcessStatus::{GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS},
            Threading::GetCurrentProcess,
        };
        // SAFETY: the struct is zeroed with `cb` set to its size, as the API requires.
        unsafe {
            let mut c: PROCESS_MEMORY_COUNTERS = std::mem::zeroed();
            c.cb = std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32;
            if GetProcessMemoryInfo(GetCurrentProcess(), &mut c, c.cb) != 0 {
                c.PeakWorkingSetSize as u64
            } else {
                0
            }
        }
    }
}
