use crate::seqlock::SeqLock;
use crate::{
    BufferManageGuardUpgrade, BufferManager, BufferManagerGuard, ExclusiveGuard, OPtr, OlcErrorHandler, OlcVersion,
    OptimisticGuard, PageId, UnwindOlcEh,
};
use bytemuck::Zeroable;
use std::cell::UnsafeCell;
use std::mem::{forget, MaybeUninit};
use std::ops::{Deref, DerefMut};
use std::sync::Mutex;

pub struct SimpleBm<P> {
    pages: Box<[UnsafeCell<P>]>,
    locks: Box<[SeqLock]>,
    free_list: Mutex<Vec<usize>>,
}

unsafe impl<P> Sync for SimpleBm<P> {}

impl<P: Zeroable> SimpleBm<P> {
    pub fn new(capacity: usize) -> Self {
        unsafe {
            SimpleBm {
                pages: Box::<[MaybeUninit<_>]>::assume_init(Box::new_zeroed_slice(capacity)),
                locks: Box::<[MaybeUninit<_>]>::assume_init(Box::new_zeroed_slice(capacity)),
                free_list: Mutex::new((0..capacity).collect()),
            }
        }
    }
}

impl<'bm, P> CommonSeqLockBM<'bm> for &'bm SimpleBm<P> {
    type Page = P;
    type OlcEH = UnwindOlcEh;

    fn pid_from_address(self, address: usize) -> PageId {
        let start = self.pages.as_ptr().addr();
        debug_assert!(address >= start);
        debug_assert!(address < start + size_of::<P>() * self.pages.len());
        let offset = address - start;
        assert_eq!(offset % size_of::<P>(), 0);
        PageId { x: (offset / size_of::<P>()) as u64 }
    }

    fn alloc(self) -> PageId {
        let pid = self.free_list.lock().unwrap().pop().expect("out of pages");
        self.locks[pid].force_lock_exclusive();
        PageId { x: pid as u64 }
    }

    fn dealloc(self, pid: PageId) {
        let pid = pid.x as usize;
        self.locks[pid].unlock_exclusive();
        self.free_list.lock().unwrap().push(pid)
    }

    fn page(self, pid: PageId) -> &'bm UnsafeCell<Self::Page> {
        &self.pages[pid.x as usize]
    }

    fn lock(self, pid: PageId) -> &'bm SeqLock {
        &self.locks[pid.x as usize]
    }
}

pub trait CommonSeqLockBM<'bm>: Copy + Sync + Send + 'bm {
    type Page;
    type OlcEH: OlcErrorHandler;
    fn pid_from_address(self, address: usize) -> PageId;
    /// acquires exclusive lock
    fn alloc(self) -> PageId;
    /// releases exclusive lock
    fn dealloc(self, pid: PageId);
    fn page(self, pid: PageId) -> &'bm UnsafeCell<Self::Page>;
    fn lock(self, pid: PageId) -> &'bm SeqLock;
}

pub struct SimpleGuardO<'bm, BM: CommonSeqLockBM<'bm>> {
    bm: BM,
    ptr: OPtr<'bm, BM::Page, BM::OlcEH>,
    version: OlcVersion,
}

impl<'bm, BM: CommonSeqLockBM<'bm>> Clone for SimpleGuardO<'bm, BM> {
    fn clone(&self) -> Self {
        SimpleGuardO { bm: self.bm, ptr: self.ptr, version: self.version }
    }
}

pub struct SimpleGuardS<'bm, BM: CommonSeqLockBM<'bm>> {
    bm: BM,
    ptr: &'bm BM::Page,
}

impl<'bm, BM: CommonSeqLockBM<'bm>> BufferManagerGuard<'bm, BM> for SimpleGuardS<'bm, BM> {
    fn acquire_wait(bm: BM, page_id: PageId) -> Self {
        let Ok(_) = bm.lock(page_id).lock_shared(());
        SimpleGuardS { bm, ptr: unsafe { &*bm.page(page_id).get() } }
    }

    fn acquire_wait_version(bm: BM, page_id: PageId, v: OlcVersion) -> Option<Self> {
        bm.lock(page_id).lock_shared(v).ok()?;
        Some(SimpleGuardS { bm, ptr: unsafe { &*bm.page(page_id).get() } })
    }

    fn release(self) -> OlcVersion {
        let version = self.bm.lock(self.page_id()).unlock_shared();
        forget(self);
        version
    }

    fn page_id(&self) -> PageId {
        self.bm.pid_from_address((self.ptr as *const BM::Page).addr())
    }

    fn o_ptr(&mut self) -> OPtr<'_, BM::Page, BM::OlcEH> {
        unsafe { OPtr::from_ref(self.ptr) }
    }
}

impl<'bm, BM: CommonSeqLockBM<'bm>> Deref for SimpleGuardS<'bm, BM> {
    type Target = BM::Page;

    fn deref(&self) -> &Self::Target {
        self.ptr
    }
}

pub struct SimpleGuardX<'bm, BM: CommonSeqLockBM<'bm>> {
    bm: BM,
    ptr: &'bm mut BM::Page,
    written: bool,
}

impl<'bm, BM: CommonSeqLockBM<'bm>> BufferManagerGuard<'bm, BM> for SimpleGuardX<'bm, BM> {
    fn acquire_wait(bm: BM, page_id: PageId) -> Self {
        let Ok(_version) = bm.lock(page_id).lock_exclusive(());
        SimpleGuardX { bm, ptr: unsafe { &mut *bm.page(page_id).get() }, written: false }
    }

    fn acquire_wait_version(bm: BM, page_id: PageId, version: OlcVersion) -> Option<Self> {
        bm.lock(page_id).lock_exclusive(version).ok()?;
        Some(SimpleGuardX { bm, ptr: unsafe { &mut *bm.page(page_id).get() }, written: false })
    }

    fn release(self) -> OlcVersion {
        let version = self.bm.lock(self.page_id()).unlock_exclusive();
        forget(self);
        version
    }

    fn page_id(&self) -> PageId {
        self.bm.pid_from_address((self.ptr as *const BM::Page).addr())
    }

    fn o_ptr(&mut self) -> OPtr<'_, BM::Page, BM::OlcEH> {
        OPtr::from_mut(self.ptr)
    }
}

impl<'bm, BM: CommonSeqLockBM<'bm>> ExclusiveGuard<'bm, BM> for SimpleGuardX<'bm, BM> {
    fn reset_written(&mut self) {
        self.written = false;
    }

    fn dealloc(self) {
        self.bm.dealloc(self.page_id());
        forget(self);
    }
}

impl<'bm, BM: CommonSeqLockBM<'bm>> Deref for SimpleGuardX<'bm, BM> {
    type Target = BM::Page;

    fn deref(&self) -> &Self::Target {
        self.ptr
    }
}

impl<'bm, BM: CommonSeqLockBM<'bm>> DerefMut for SimpleGuardX<'bm, BM> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.written = true;
        self.ptr
    }
}

impl<'bm, BM: CommonSeqLockBM<'bm>> BufferManager<'bm> for BM {
    type Page = <Self as CommonSeqLockBM<'bm>>::Page;
    type OlcEH = <Self as CommonSeqLockBM<'bm>>::OlcEH;
    type GuardO = SimpleGuardO<'bm, Self>;
    type GuardS = SimpleGuardS<'bm, Self>;
    type GuardX = SimpleGuardX<'bm, Self>;

    fn alloc(self) -> Self::GuardX {
        let pid = self.alloc();
        SimpleGuardX { bm: self, ptr: unsafe { &mut *self.page(pid).get() }, written: false }
    }
}

impl<'bm, BM: CommonSeqLockBM<'bm>> BufferManageGuardUpgrade<'bm, BM, SimpleGuardS<'bm, BM>> for SimpleGuardO<'bm, BM> {
    fn upgrade(self) -> SimpleGuardS<'bm, BM> {
        let pid = self.bm.pid_from_address(self.ptr.to_raw().addr());
        BM::OlcEH::optmistic_fail_check(self.bm.lock(pid).lock_shared(self.version));
        let ret = SimpleGuardS { bm: self.bm, ptr: unsafe { &*self.bm.page(pid).get() } };
        self.release_unchecked();
        ret
    }
}

impl<'bm, BM: CommonSeqLockBM<'bm>> BufferManageGuardUpgrade<'bm, BM, SimpleGuardX<'bm, BM>> for SimpleGuardO<'bm, BM> {
    fn upgrade(self) -> SimpleGuardX<'bm, BM> {
        let pid = self.bm.pid_from_address(self.ptr.to_raw().addr());
        BM::OlcEH::optmistic_fail_check(self.bm.lock(pid).lock_exclusive(self.version));
        let ret = SimpleGuardX { bm: self.bm, ptr: unsafe { &mut *self.bm.page(pid).get() }, written: false };
        self.release_unchecked();
        ret
    }
}

impl<'bm, BM: CommonSeqLockBM<'bm>> OptimisticGuard<'bm, BM> for SimpleGuardO<'bm, BM> {
    fn release_unchecked(self) {
        forget(self);
    }

    fn check(&self) -> OlcVersion {
        BM::OlcEH::optmistic_fail_check(
            self.bm.lock(self.bm.pid_from_address(self.ptr.to_raw().addr())).try_unlock_optimistic(self.version),
        );
        self.version
    }

    fn o_ptr_bm(&self) -> OPtr<'bm, BM::Page, BM::OlcEH> {
        self.ptr
    }
}

impl<'bm, BM: CommonSeqLockBM<'bm>> Drop for SimpleGuardO<'bm, BM> {
    fn drop(&mut self) {
        match self.bm.lock(self.bm.pid_from_address(self.ptr.to_raw().addr())).try_unlock_optimistic(self.version) {
            Ok(_) => (),
            Err(e) => {
                if !BM::OlcEH::is_unwinding() {
                    BM::OlcEH::optimistic_fail_with(e);
                }
            }
        }
    }
}

impl<'bm, BM: CommonSeqLockBM<'bm>> Drop for SimpleGuardS<'bm, BM> {
    fn drop(&mut self) {
        self.bm.lock(self.page_id()).unlock_shared();
    }
}

impl<'bm, BM: CommonSeqLockBM<'bm>> Drop for SimpleGuardX<'bm, BM> {
    fn drop(&mut self) {
        if BM::OlcEH::is_unwinding() {
            assert!(!self.written);
        }
        self.bm.lock(self.page_id()).unlock_exclusive();
    }
}

impl<'bm, BM: CommonSeqLockBM<'bm>> BufferManagerGuard<'bm, BM> for SimpleGuardO<'bm, BM> {
    fn acquire_wait(bm: BM, page_id: PageId) -> Self {
        let Ok(version) = bm.lock(page_id).lock_optimistic(());
        SimpleGuardO { bm, ptr: unsafe { OPtr::from_raw(bm.page(page_id).get()) }, version }
    }

    fn acquire_wait_version(bm: BM, page_id: PageId, version: OlcVersion) -> Option<Self> {
        bm.lock(page_id).lock_optimistic(version).ok()?;
        Some(SimpleGuardO { bm, ptr: unsafe { OPtr::from_raw(bm.page(page_id).get()) }, version })
    }

    fn release(self) -> OlcVersion {
        BM::OlcEH::optmistic_fail_check(self.bm.lock(self.page_id()).try_unlock_optimistic(self.version));
        let version = self.version;
        forget(self);
        version
    }

    fn page_id(&self) -> PageId {
        self.bm.pid_from_address(self.ptr.to_raw() as usize)
    }

    fn o_ptr(&mut self) -> OPtr<'_, BM::Page, BM::OlcEH> {
        self.ptr
    }
}
