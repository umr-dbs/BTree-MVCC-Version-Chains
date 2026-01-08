use std::cell::UnsafeCell;
use std::fmt::{Display, Formatter};
use std::mem;
use std::ops::{Deref, DerefMut};

/**
 *
 * Created by   Amir El-Shaikh on 04.03.2021.
 * E-Mail: elshaikh@mathematik.uni-marburg.de
 *
 * @Author: Amir El-Shaikh
 *
 */
// Copied from ChronicleDB and adapted.
#[derive(Default)]
pub struct UnCell<E: Default> {
    inner: UnsafeCell<E>
}

impl<E: Default + Display> Display for UnCell<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "UnCell({})", self.get_mut())
    }
}

/// Impl. Block for SafeCell and for all E.
impl<E: Default> UnCell<E> {
    /// Unsafely wraps the e.
    pub const fn new(e: E) -> Self {
        Self {
            inner: UnsafeCell::new(e)
        }
    }

    /// Unsafely unwraps the e.
    pub fn into_inner(self) -> E {
        self.inner.into_inner()
    }

    #[inline]
    pub fn replace(&self, new: E) -> E {
        mem::replace(self.get_mut(), new)
    }

    /// Unsafely gets the wrapped object as mutable reference.
    #[inline]
    pub fn get_mut(&self) -> &mut E {
        unsafe { &mut *self.inner.get() }
    }

    /// Unsafely gets the wrapped object as an immutable reference.
    #[inline]
    pub fn get(&self) -> &E {
        unsafe { &*self.inner.get() }
    }
}

/// Implements AsRef for SafeCell.
impl<T: Default> AsRef<T> for UnCell<T> {
    /// Unsafely gets the wrapped object as reference.
    fn as_ref(&self) -> &T {
        unsafe { &*self.inner.get() }
    }
}

/// Implements AsMut for SafeCell.
impl<T: Default> AsMut<T> for UnCell<T> {
    /// Unsafely gets the wrapped object as mutable reference.
    fn as_mut(&mut self) -> &mut T {
        unsafe { &mut *self.inner.get() }
    }
}

/// Implements Deref for SafeCell, allowing auto deref.
impl<T: Default> Deref for UnCell<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

/// Implements DerefMut for SafeCell, allowing auto mutable deref.
impl<T: Default> DerefMut for UnCell<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}

/// Explicitly allow concurrent programming.
unsafe impl<E: Default> Sync for UnCell<E> {}

/// Explicitly allow concurrent programming.
unsafe impl<E: Default> Send for UnCell<E> {}