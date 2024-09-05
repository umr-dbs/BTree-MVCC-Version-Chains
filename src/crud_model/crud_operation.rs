use std::fmt::{Display, Formatter};
use std::hash::Hash;
// use crate::record_model::record_like::RecordLike;
// use crate::record_model::Version;
use crate::utils::interval::Interval;
use crate::crud_model::crud_operation::CRUDOperation::{Empty, Delete, Point, Insert, Range, Update};

/// Transactions definitions.
/// Empty variant indicates an initiation error and/or a default stack allocation.
#[derive(Clone, Default)]
pub enum CRUDOperation<Key: Ord + Copy + Hash, Payload: Clone> {
    #[default]
    Empty,

    Insert(Key, Payload),
    Update(Key, Payload),
    Delete(Key),
    Point(Key),
    PeekMin,
    PeekMax,
    PopMin,
    PopMax,
    Range(Interval<Key>),
}

/// Explicitly support move-semantics for Transaction.
unsafe impl<Key: Ord + Copy + Hash, Payload: Clone> Send for CRUDOperation<Key, Payload> {}
unsafe impl<Key: Ord + Copy + Hash, Payload: Clone> Sync for CRUDOperation<Key, Payload> {}
/// Implements Display for Transaction, i.e. pretty printers.
impl<Key: Display + Ord + Copy + Hash, Payload: Display + Clone> Display for CRUDOperation<Key, Payload> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Insert(key, payload) =>
                write!(f, "Insert(Key: {}, Payload: {})", key, payload),
            Update(key, payload) =>
                write!(f, "Update(key: {}, payload: {})", key, payload),
            Delete(key) =>
                write!(f, "Delete(Key: {})", key),
            Point(key) =>
                write!(f, "Point(Key: {})", key),
            Range(key) =>
                write!(f, "Range(Keys: [{}, {}])", key.lower(), key.upper()),
            Empty => write!(f, "Empty"),
            CRUDOperation::PeekMin => 
                write!(f, "MinPoint"),
            CRUDOperation::PeekMax =>
                write!(f, "MaxPoint"),
            CRUDOperation::PopMin => 
                write!(f, "PopMin"),
            CRUDOperation::PopMax =>
                write!(f, "PopMax"),
        }
    }
}

/// Main implementation block for Transaction.
impl<Key: Ord + Hash + Copy, Payload: Clone> CRUDOperation<Key, Payload> {
    /// Returns true, only if the Transaction does not require write access when executing.
    /// Returns false, otherwise.
    #[inline(always)]
    pub const fn is_read(&self) -> bool {
        match self {
            Insert(..) | Delete(..) | Update(..) => false,
            _ => true,
        }
    }

    /// Returns true, only if the Transaction requires write access when executing.
    /// Returns false, otherwise.
    #[inline(always)]
    pub const fn is_write(&self) -> bool {
        !self.is_read()
    }
}
