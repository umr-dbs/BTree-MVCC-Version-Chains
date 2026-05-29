use std::cell::Cell;
use std::fmt::Display;
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem;
use std::mem::MaybeUninit;
use crate::mvb_page_model::ObjectCount;
use crate::mvb_record_model::v_record_point::VersionedRecordPoint;
use crate::mvb_utils::safe_cell::SafeCell;
use crate::mvb_utils::shadow_vec::ShadowVec;

pub struct LeafPage<
    const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default,
    Payload: Default + Clone + Send + Sync + Display + 'static
> {
    pub(crate) records_len: SafeCell<ObjectCount>,
    pub(crate) record_data: [MaybeUninit<VersionedRecordPoint<Key, Payload>>; NUM_RECORDS],
    _marker: PhantomData<(Key, Payload)>,
}

impl<const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default,
    Payload: Default + Clone + Send + Sync + Display + 'static
> Default for LeafPage<NUM_RECORDS, Key, Payload> {
    fn default() -> Self {
        LeafPage::new()
    }
}

impl<const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default,
    Payload: Default + Clone + Send + Sync + Display + 'static
> Drop for LeafPage<NUM_RECORDS, Key, Payload> {
    fn drop(&mut self) {
        self.as_records_mut()
            .clear();
    }
}

impl<const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default,
    Payload: Default + Clone + Send + Sync + Display + 'static
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
    pub fn as_records(&self) -> &[VersionedRecordPoint<Key, Payload>] {
        unsafe {
            std::slice::from_raw_parts(self.record_data.as_ptr() as *const VersionedRecordPoint<Key, Payload>,
                                       *self.records_len.get_mut() as _)
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        *self.records_len.get_mut() as _
    }

    #[inline(always)]
    pub fn set_len(&self, len: usize) {
        *self.records_len.get_mut() = len as _
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
    pub fn as_records_mut(&self) -> ShadowVec<VersionedRecordPoint<Key, Payload>> {
        ShadowVec {
            ptr: self.record_data.as_ptr() as *mut VersionedRecordPoint<Key, Payload>,
            len: SafeCell::new(*self.records_len.get_mut() as _),
            update_len: Some(self.records_len.get_mut()),
        }
    }
}