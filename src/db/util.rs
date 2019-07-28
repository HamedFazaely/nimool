use std::sync::{Mutex, Arc, MutexGuard, PoisonError};


pub fn acquire_mutex_lock<'a, T, F>(lock: &'a Mutex<T>, on_poisoned: Option<F>) -> MutexGuard<'a, T>
    where F: FnOnce(PoisonError<MutexGuard<T>>) -> MutexGuard<T>
{
    match lock.lock() {
        Ok(inner) => inner,
        Err(e) => {
            if let Some(f) = on_poisoned {
                f(e)
            } else {
                warn!("poisoned lock: {:?}", e);
                e.into_inner()
            }
        }
    }
}