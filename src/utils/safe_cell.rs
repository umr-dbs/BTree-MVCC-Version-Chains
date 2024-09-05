use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};

/**
 *
 * Created by   Amir El-Shaikh on 04.03.2021.
 * E-Mail: elshaikh@mathematik.uni-marburg.de
 *
 * @Author: Amir El-Shaikh
 *
 */

/// Experimental: Remove AtomicRefCell dependency and sync it yourself.
pub struct SafeCell<E> {
    inner: UnsafeCell<E>
}

/// Impl. Block for SafeCell and for all E.
impl<E> SafeCell<E> {
    /// Unsafely wraps the e.
    #[inline(always)]
    pub const fn new(e: E) -> Self {
        Self {
            inner: UnsafeCell::new(e)
        }
    }

    /// Unsafely unwraps the e.
    #[inline(always)]
    pub fn into_inner(self) -> E {
        self.inner.into_inner()
    }

    /// Unsafely gets the wrapped object as mutable reference.
    #[inline(always)]
    pub fn get_mut(&self) -> &mut E {
        unsafe { &mut *self.inner.get() }
    }
}

/// Implements AsRef for SafeCell.
impl<T> AsRef<T> for SafeCell<T> {
    /// Unsafely gets the wrapped object as reference.
    #[inline(always)]
    fn as_ref(&self) -> &T {
        unsafe { &*self.inner.get() }
    }
}

/// Implements AsMut for SafeCell.
impl<T> AsMut<T> for SafeCell<T> {
    /// Unsafely gets the wrapped object as mutable reference.
    #[inline(always)]
    fn as_mut(&mut self) -> &mut T {
        unsafe { &mut *self.inner.get() }
    }
}

/// Implements Deref for SafeCell, allowing auto deref.
impl<T> Deref for SafeCell<T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

/// Implements DerefMut for SafeCell, allowing auto mutable deref.
impl<T> DerefMut for SafeCell<T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}

/// Explicitly allow concurrent programming.
unsafe impl<E> Sync for SafeCell<E> {}

/// Explicitly allow concurrent programming.
unsafe impl<E> Send for SafeCell<E> {}