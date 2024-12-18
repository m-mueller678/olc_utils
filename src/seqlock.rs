use crate::{OlcVersion, OptimisticError};
use bytemuck::Zeroable;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{fence, AtomicU64};

#[derive(Zeroable)]
pub struct SeqLock(AtomicU64);

const COUNT_BITS: u32 = 10;
const COUNT_MASK: u64 = (1 << COUNT_BITS) - 1;
const EXCLUSIVE_MASK: u64 = 1 << COUNT_BITS;
const VERSION_SHIFT: u32 = COUNT_BITS + 1;

pub trait VersionFilter: Copy {
    type E;
    type R;
    fn check(self, v: u64) -> Result<(), Self::E>;
    fn map_r(self, v: u64) -> Self::R;
}

impl VersionFilter for () {
    type E = !;
    type R = OlcVersion;
    fn check(self, _v: u64) -> Result<(), Self::E> {
        Ok(())
    }

    fn map_r(self, v: u64) -> Self::R {
        OlcVersion { x: v }
    }
}

impl VersionFilter for OlcVersion {
    type E = OptimisticError;
    type R = ();

    fn check(self, v: u64) -> Result<Self::R, Self::E> {
        if v == self.x {
            Ok(())
        } else {
            Err(OptimisticError::new())
        }
    }

    fn map_r(self, v: u64) -> Self::R {
        debug_assert!(v == self.x);
    }
}

impl Default for SeqLock {
    fn default() -> Self {
        Self::new()
    }
}

impl SeqLock {
    pub fn new() -> Self {
        SeqLock(AtomicU64::new(0))
    }
    pub fn lock_shared<F: VersionFilter>(&self, f: F) -> Result<F::R, F::E> {
        lock_track_check(self, Some(false));
        let mut x = self.0.load(Relaxed);
        loop {
            f.check(x >> VERSION_SHIFT)?;
            if x & (COUNT_MASK | EXCLUSIVE_MASK) < COUNT_MASK {
                match self.0.compare_exchange_weak(x, x + 1, Acquire, Relaxed) {
                    Ok(_) => {
                        lock_track_set(self, Some(false));
                        return Ok(f.map_r(x >> VERSION_SHIFT));
                    }
                    Err(v) => x = v,
                }
            } else {
                self.wait();
            }
        }
    }

    pub fn unlock_shared(&self) -> OlcVersion {
        lock_track_set(self, None);
        let fetched = self.0.fetch_sub(1, Release);
        debug_assert!(fetched & COUNT_MASK != 0);
        OlcVersion { x: fetched >> VERSION_SHIFT }
    }

    fn wait(&self) {
        //TODO
        std::thread::yield_now();
    }

    /// returns version before locking
    pub fn lock_exclusive<F: VersionFilter>(&self, f: F) -> Result<F::R, F::E> {
        lock_track_check(self, Some(true));
        loop {
            let mut x = self.0.load(Relaxed);
            f.check(x >> VERSION_SHIFT)?;
            if x & EXCLUSIVE_MASK == 0 {
                x = self.0.fetch_or(EXCLUSIVE_MASK, Acquire);
                if x & EXCLUSIVE_MASK != 0 {
                    self.wait();
                    continue;
                }
                if f.check(x >> VERSION_SHIFT).is_err() {
                    self.0.fetch_and(!EXCLUSIVE_MASK, Relaxed);
                    self.wait();
                    continue;
                }
                if x & (EXCLUSIVE_MASK | COUNT_MASK) == 0 {
                    lock_track_set(self, Some(true));
                    return Ok(f.map_r(x >> VERSION_SHIFT));
                }
                loop {
                    self.wait();
                    x = self.0.load(Acquire);
                    if x & COUNT_MASK == 0 {
                        lock_track_set(self, Some(true));
                        return Ok(f.map_r(x >> VERSION_SHIFT));
                    }
                }
            }
        }
    }

    pub fn force_lock_exclusive(&self) -> OlcVersion {
        lock_track_check(self, Some(true));
        lock_track_set(self, Some(true));
        let x = self.0.fetch_or(EXCLUSIVE_MASK, Acquire);
        debug_assert!(x & (EXCLUSIVE_MASK | COUNT_MASK) == 0);
        OlcVersion { x: x >> VERSION_SHIFT }
    }

    /// returns version after unlocking
    pub fn unlock_exclusive(&self) -> OlcVersion {
        lock_track_set(self, None);
        let fetched = self.0.fetch_add(EXCLUSIVE_MASK, Release);
        debug_assert!(fetched & EXCLUSIVE_MASK != 0);
        OlcVersion { x: (fetched + EXCLUSIVE_MASK) >> VERSION_SHIFT }
    }

    pub fn lock_optimistic<F: VersionFilter>(&self, f: F) -> Result<F::R, F::E> {
        lock_track_check(self, None);
        loop {
            let x = self.0.load(Acquire);
            f.check(x >> VERSION_SHIFT)?;
            if x & EXCLUSIVE_MASK == 0 {
                return Ok(f.map_r(x >> VERSION_SHIFT));
            } else {
                self.wait();
            }
        }
    }

    pub fn try_unlock_optimistic(&self, v: OlcVersion) -> Result<(), OptimisticError> {
        fence(Acquire);
        let x = self.0.load(Relaxed);
        if (x & !COUNT_MASK) == v.x << VERSION_SHIFT {
            Ok(())
        } else {
            Err(OptimisticError::new())
        }
    }
}

#[cfg(not(feature = "track-thread-locks"))]
fn lock_track_check(_lock: &SeqLock, _mode: Option<bool>) {}
#[cfg(not(feature = "track-thread-locks"))]
fn lock_track_set(_lock: &SeqLock, _mode: Option<bool>) {}

#[cfg(feature = "track-thread-locks")]
use track_tread_locks::*;

#[cfg(feature = "track-thread-locks")]
mod track_tread_locks {
    use super::SeqLock;
    use std::cell::RefCell;
    use std::collections::HashMap;

    std::thread_local! {
        static THREAD_LOCKS:RefCell<HashMap<usize,bool>>=Default::default();
    }

    pub fn lock_track_check(lock: &SeqLock, mode: Option<bool>) {
        let addr = (lock as *const SeqLock).addr();
        let existing = THREAD_LOCKS.with_borrow(|m| m.get(&addr).copied());
        if existing.is_some() {
            panic!("cannot acquire {} lock because {} is held by same thread", lock_name(mode), lock_name(existing))
        }
    }

    pub fn lock_track_set(lock: &SeqLock, mode: Option<bool>) {
        let addr = (lock as *const SeqLock).addr();
        THREAD_LOCKS.with_borrow_mut(|m| {
            if let Some(mode) = mode {
                m.insert(addr, mode);
            } else {
                m.remove(&addr);
            }
        });
    }

    fn lock_name(mode: Option<bool>) -> &'static str {
        match mode {
            None => "optimistic",
            Some(false) => "shared",
            Some(true) => "exclusive",
        }
    }
}
