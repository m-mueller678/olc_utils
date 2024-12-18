use crate::optimistic_error::OlcErrorHandler;
use bytemuck::Pod;
use radium::marker::Atomic;
use radium::Radium;
use std::cell::UnsafeCell;
use std::cmp::Ordering;
use std::ffi::c_void;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr::slice_from_raw_parts;
use std::slice::SliceIndex;
use std::sync::atomic::Ordering::Relaxed;

impl<T: ?Sized, O: OlcErrorHandler> Copy for OPtr<'_, T, O> {}
impl<T: ?Sized, O: OlcErrorHandler> Clone for OPtr<'_, T, O> {
    fn clone(&self) -> Self {
        *self
    }
}

// TODO add type parameter that makes OPtr not optimistic

pub struct OPtr<'a, T: ?Sized, O: OlcErrorHandler> {
    p: *const T,
    _p: PhantomData<&'a T>,
    _bm: PhantomData<O>,
}

impl<'a, T, O: OlcErrorHandler> OPtr<'a, T, O> {
    pub fn to_raw(self) -> *const T {
        self.p
    }

    pub fn from_mut(x: &'a mut T) -> Self {
        OPtr { p: x as *const T, _p: PhantomData, _bm: PhantomData }
    }

    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn from_ref(x: &'a T) -> Self {
        OPtr { p: x as *const T, _p: PhantomData, _bm: PhantomData }
    }

    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn from_raw(p: *const T) -> Self {
        OPtr { p, _p: PhantomData, _bm: PhantomData }
    }

    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn project<R>(self, f: impl FnOnce(*const T) -> *const R) -> OPtr<'a, R, O> {
        OPtr { p: f(self.p), _p: PhantomData, _bm: PhantomData }
    }

    pub fn cast<U>(self) -> OPtr<'a, U, O> {
        assert_eq!(size_of::<T>(), size_of::<U>());
        assert!(align_of::<T>() >= align_of::<U>());
        OPtr { p: self.p as *const U, _p: PhantomData, _bm: PhantomData }
    }

    pub fn array_slice<const L: usize>(self, offset: usize) -> OPtr<'a, [u8; L], O> {
        assert!(L <= size_of::<T>());
        if offset > size_of::<T>() - L {
            O::optimistic_fail()
        }
        unsafe { OPtr { p: (self.p as *const u8).add(offset) as *const [u8; L], _bm: PhantomData, _p: PhantomData } }
    }

    pub fn as_slice<U: Pod>(self) -> OPtr<'a, [U], O> {
        assert_eq!(size_of::<T>() % size_of::<U>(), 0);
        assert!(align_of::<T>() >= align_of::<U>());
        OPtr {
            p: slice_from_raw_parts(self.p as *const U, size_of::<T>() / size_of::<U>()),
            _p: PhantomData,
            _bm: PhantomData,
        }
    }

    pub fn read_unaligned_nonatomic_u16(self, offset: usize) -> usize {
        if offset + 2 <= size_of::<T>() {
            unsafe { ((self.p as *const u8).add(offset) as *const u16).read_unaligned() as usize }
        } else {
            O::optimistic_fail()
        }
    }

    pub fn read_unaligned_nonatomic_u64(self, offset: usize) -> u64 {
        if offset + 8 <= size_of::<T>() {
            unsafe { ((self.p as *const u8).add(offset) as *const u64).read_unaligned() }
        } else {
            O::optimistic_fail()
        }
    }

    pub fn r(self) -> T
    where
        T: Atomic + Pod,
    {
        unsafe { (*(self.p as *const T::Atom)).load(Relaxed) }
    }
}

impl<'a, T: Pod, O: OlcErrorHandler> OPtr<'a, [T], O> {
    pub fn i<I: Clone + SliceIndex<[T]> + SliceIndex<[UnsafeCell<T>]>>(
        self,
        i: I,
    ) -> OPtr<'a, <I as SliceIndex<[T]>>::Output, O> {
        unsafe {
            let p = slice_from_raw_parts(self.p as *const UnsafeCell<T>, self.p.len());
            if (*p).get(i.clone()).is_none() {
                // bounds check
                O::optimistic_fail()
            };
            OPtr { p: i.get_unchecked(self.p), _p: PhantomData, _bm: PhantomData }
        }
    }

    pub fn sub(self, offset: usize, len: usize) -> OPtr<'a, [T], O> {
        self.i(offset..offset + len)
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(self) -> usize {
        self.p.len()
    }
}

impl<'a, T: Pod, O: OlcErrorHandler, const N: usize> OPtr<'a, [T; N], O> {
    pub fn unsize(self) -> OPtr<'a, [T], O> {
        OPtr { p: self.p.as_slice(), _p: PhantomData, _bm: PhantomData }
    }
}

impl<O: OlcErrorHandler> OPtr<'_, [u8], O> {
    pub fn load_bytes(self, dst: &mut [u8]) {
        assert_eq!(self.p.len(), dst.len());
        unsafe { std::ptr::copy(self.p as *const u8, dst.as_mut_ptr(), self.p.len()) }
    }

    pub fn load_bytes_uninit(self, dst: &mut [MaybeUninit<u8>]) -> &mut [u8] {
        unsafe {
            assert_eq!(self.p.len(), dst.len());
            std::ptr::copy(self.p as *const u8, dst.as_mut_ptr() as *mut u8, self.p.len());
            MaybeUninit::slice_assume_init_mut(dst)
        }
    }

    pub fn load_slice_to_vec(self) -> Vec<u8> {
        let mut dst = vec![0u8; self.p.len()];
        self.load_bytes(&mut dst);
        dst
    }

    pub fn mem_cmp(self, other: &[u8]) -> Ordering {
        unsafe {
            let cmp_len = self.len().min(other.len());
            let r = libc::memcmp(self.p as *const u8 as *const c_void, other.as_ptr() as *const c_void, cmp_len);
            r.cmp(&0).then(self.len().cmp(&other.len()))
        }
    }
}

#[macro_export]
macro_rules! o_project {
    ($this:ident$(.$member:ident)+) => {
        {
            let ptr:OPtr<_,_> = $this;
            unsafe{ptr.project(|p|{
                // TODO make sure you cannot sneak in a union field access here
                &raw const (*p)$(.$member)+
            })}
        }
    };
}
