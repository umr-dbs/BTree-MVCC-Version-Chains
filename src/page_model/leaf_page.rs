use std::cell::Cell;
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem;
use std::mem::{ManuallyDrop, MaybeUninit};
use crate::page_model::ObjectCount;
use crate::record_model::record_point::RecordPoint;
use crate::utils::safe_cell::SafeCell;
use crate::utils::shadow_vec::ShadowVec;

pub struct LeafPage<
    const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default,
> {
    pub(crate) records_len: SafeCell<ObjectCount>,
    pub(crate) record_data: [MaybeUninit<RecordPoint<Key, Payload>>; NUM_RECORDS],
    _marker: PhantomData<(Key, Payload)>,
}

impl<const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default,
> Default for LeafPage<NUM_RECORDS, Key, Payload> {
    fn default() -> Self {
        LeafPage::new()
    }
}

impl<const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default
> Drop for LeafPage<NUM_RECORDS, Key, Payload> {
    fn drop(&mut self) {
        self.as_records_mut()
            .clear();
    }
}

impl<const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default,
> LeafPage<NUM_RECORDS, Key, Payload> {
    #[inline(always)]
    pub const fn new() -> Self {
        Self {
            records_len: SafeCell::new(0),
            record_data: unsafe { mem::MaybeUninit::uninit().assume_init() }, // <[MaybeUninit<Entry>; NUM_RECORDS]>::
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub fn as_records(&self) -> &[RecordPoint<Key, Payload>] {
        unsafe {
            std::slice::from_raw_parts(self.record_data.as_ptr() as *const RecordPoint<Key, Payload>,
                                       *self.records_len.get_mut() as _)
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        *self.records_len.get_mut() as _
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.len() == NUM_RECORDS
    }

    #[inline(always)]
    pub fn as_records_mut(&self) -> ShadowVec<RecordPoint<Key, Payload>> {
        ShadowVec {
            ptr: self.record_data.as_ptr() as *mut RecordPoint<Key, Payload>,
            len: Cell::new(*self.records_len.get_mut() as _),
            update_len: Some(self.records_len.get_mut()),
        }
    }
}