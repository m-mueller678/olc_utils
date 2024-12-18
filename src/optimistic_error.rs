use std::fmt::{Display, Formatter};
use std::panic::{catch_unwind, resume_unwind, UnwindSafe};

impl Display for OptimisticError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("optimistic error")
    }
}

pub struct OptimisticError {
    _private: (),
}

impl OptimisticError {
    pub(crate) fn new() -> Self {
        OptimisticError { _private: () }
    }
}

pub trait OlcErrorHandler {
    fn optmistic_fail_check(r: Result<(), OptimisticError>) {
        if let Err(e) = r {
            Self::optimistic_fail_with(e)
        }
    }
    fn optimistic_fail_with(e: OptimisticError) -> !;

    fn optimistic_fail() -> ! {
        Self::optimistic_fail_with(OptimisticError::new())
    }
    // TODO consider adding a marker type that is returned by functions that may unwind and marked must_use
    fn catch<R>(f: impl FnOnce() -> R) -> Result<R, OptimisticError>;

    /// Returns `true` if currently unwinding due to an optimistic error.
    /// Lock guards should use this for poisoning and to avoid calling one of the fail methods while already unwinding
    fn is_unwinding() -> bool;
}

pub struct UnwindOlcEh;

pub struct PanicOlcEh;

impl OlcErrorHandler for UnwindOlcEh {
    fn optimistic_fail_with(error: OptimisticError) -> ! {
        resume_unwind(Box::new(error));
    }

    fn catch<R>(f: impl FnOnce() -> R) -> Result<R, OptimisticError> {
        struct IgnoreUnwindSafe<X>(X);
        impl<X> UnwindSafe for IgnoreUnwindSafe<X> {}
        let f2 = IgnoreUnwindSafe(f);
        let result = catch_unwind(move || {
            let f2 = f2;
            f2.0()
        });
        match result {
            Ok(r) => Ok(r),
            Err(e) => match e.downcast::<OptimisticError>() {
                Ok(x) => Err(*x),
                Err(e) => resume_unwind(e),
            },
        }
    }

    fn is_unwinding() -> bool {
        std::thread::panicking()
    }
}

impl OlcErrorHandler for PanicOlcEh {
    fn optimistic_fail_with(e: OptimisticError) -> ! {
        panic!("{e}")
    }

    fn catch<R>(_f: impl FnOnce() -> R) -> Result<R, OptimisticError> {
        unimplemented!()
    }

    fn is_unwinding() -> bool {
        std::thread::panicking()
    }
}
