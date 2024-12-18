#![feature(slice_index_methods)]
#![feature(array_ptr_get)]
#![feature(never_type)]
#![feature(new_zeroed_alloc)]
#![feature(map_try_insert)]
#![feature(maybe_uninit_slice)]

use bytemuck::{Pod, Zeroable};
pub use o_ptr::OPtr;
pub use optimistic_error::{OlcErrorHandler, OptimisticError};
use std::ops::{Deref, DerefMut};

mod buffer_manager;
mod o_ptr;
mod optimistic_error;
mod seqlock;

pub use buffer_manager::*;
pub use optimistic_error::{PanicOlcEh, UnwindOlcEh};

#[derive(Eq, PartialEq, Clone, Copy)]
pub struct OlcVersion {
    pub x: u64,
}

#[derive(Debug, Zeroable, Copy, Clone, Eq, PartialEq, Pod)]
#[repr(transparent)]
pub struct PageId {
    pub x: u64,
}

pub trait BufferManager<'bm>: 'bm + Copy + Send + Sync + Sized {
    type Page;
    type GuardO: OptimisticGuard<'bm, Self>
        + BufferManageGuardUpgrade<'bm, Self, Self::GuardS>
        + BufferManageGuardUpgrade<'bm, Self, Self::GuardX>;
    type GuardS: BufferManagerGuard<'bm, Self> + Deref<Target = Self::Page>;
    type GuardX: ExclusiveGuard<'bm, Self> + Deref<Target = Self::Page> + DerefMut;
    type OlcEH: OlcErrorHandler;
    fn alloc(self) -> Self::GuardX;
    #[deprecated]
    fn free(self, g: Self::GuardX) {
        g.dealloc();
    }
}

pub trait BufferManagerExt<'bm>: BufferManager<'bm> {
    fn repeat<R>(mut f: impl FnMut() -> R) -> R {
        loop {
            if let Ok(x) = Self::OlcEH::catch(&mut f) {
                return x;
            }
        }
    }

    fn lock_optimistic(self, pid: PageId) -> Self::GuardO {
        Self::GuardO::acquire_wait(self, pid)
    }
    fn lock_shared(self, pid: PageId) -> Self::GuardS {
        Self::GuardS::acquire_wait(self, pid)
    }
    fn lock_exclusive(self, pid: PageId) -> Self::GuardX {
        Self::GuardX::acquire_wait(self, pid)
    }
}

impl<'bm, BM: BufferManager<'bm>> BufferManagerExt<'bm> for BM {}

pub trait BufferManagerGuard<'bm, B: BufferManager<'bm>>: Sized {
    fn acquire_wait(bm: B, page_id: PageId) -> Self;
    fn acquire_wait_version(bm: B, page_id: PageId, v: OlcVersion) -> Option<Self>;
    fn release(self) -> OlcVersion;
    fn page_id(&self) -> PageId;
    fn o_ptr(&mut self) -> OPtr<'_, B::Page, B::OlcEH>;
}

pub trait OptimisticGuard<'bm, BM: BufferManager<'bm>>: BufferManagerGuard<'bm, BM> + Clone {
    fn release_unchecked(self) {
        std::mem::forget(self)
    }
    fn check(&self) -> OlcVersion {
        self.clone().release()
    }
    fn o_ptr_bm(&self) -> OPtr<'bm, BM::Page, BM::OlcEH>;
}

pub trait ExclusiveGuard<'bm, BM: BufferManager<'bm>>: BufferManagerGuard<'bm, BM> {
    fn reset_written(&mut self);
    fn dealloc(self);
}

pub trait BufferManageGuardUpgrade<'bm, B: BufferManager<'bm>, Target>: Sized {
    fn upgrade(self) -> Target;
}
