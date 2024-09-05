use std::hash::Hash;
use std::marker::PhantomData;
use std::{mem, ptr};
use std::cell::Cell;
use std::mem::MaybeUninit;
use std::ptr::null_mut;
use crate::page_model::{BlockRef, ObjectCount};
use crate::utils::shadow_vec::ShadowVec;

pub struct InternalPage<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone
> {
    pub(crate) key_array: [MaybeUninit<Key>; FAN_OUT],
    pub(crate) children_array: [MaybeUninit<BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>>; FAN_OUT],
    _marker: PhantomData<(Key, Payload)>,
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone,
> Drop for InternalPage<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    fn drop(&mut self) {
        self.children_mut()
            .clear();

        self.keys_mut()
            .clear();
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone
> InternalPage<FAN_OUT, NUM_RECORDS, Key, Payload> {
    #[inline(always)]
    pub fn new() -> Self {
        debug_assert!(mem::size_of::<Key>() >= mem::size_of::<ObjectCount>(), "KEY_SIZE can't be under ObjectCount bytes!");

        unsafe {
            let mut page = InternalPage {
                key_array: mem::MaybeUninit::uninit().assume_init(), // ::<[MaybeUninit<Key>; FAN_OUT]>
                children_array: mem::MaybeUninit::uninit().assume_init(), // ::<[MaybeUninit<BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload, Entry>>; FAN_OUT]>
                _marker: PhantomData,
            };

            *(page.key_array.as_mut_ptr() as *mut ObjectCount) = 0;
            page
        }
    }

    #[inline(always)]
    pub fn keys_mut(&self) -> ShadowVec<Key> {
        unsafe {
            ShadowVec {
                ptr: self.key_array.as_ptr().add(1) as *mut Key,
                len: Cell::new(self.keys_len()),
                update_len: Some(self.key_array.as_ptr() as *mut ObjectCount),
            }
        }
    }

    #[inline(always)]
    pub fn children_mut(&self) -> ShadowVec<BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>> {
        ShadowVec {
            ptr: self.children_array.as_ptr() as _,
            len: Cell::new(self.children_len()),
            update_len: None,
        }
    }

    #[inline(always)]
    pub const fn get_key(&self, index: usize) -> Key {
        unsafe {
            *(self.key_array.as_ptr().add(index + 1) as *const Key)
            // ptr::read_unaligned(self.key_array.as_ptr().add(index + 1) as *const Key)
        }
    }

    #[inline(always)]
    pub const fn get_key_raw(&self, index: usize) -> MaybeUninit<Key> {
        unsafe {
            *(self.key_array.as_ptr().add(index + 1))
        }
    }

    #[inline(always)]
    pub fn get_child_result(&self, index: usize) -> MaybeUninit<BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>> {
        unsafe {
            mem::transmute_copy(self.children_array.get_unchecked(index))
        }
    }

    #[inline(always)]
    pub unsafe fn get_child_unsafe(&self, index: usize) -> &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.children_array
            .get_unchecked(index)
            .assume_init_ref()
    }

    #[inline(always)]
    pub unsafe fn get_child_unsafe_cloned(&self, index: usize) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.get_child_unsafe(index)
            .clone()
    }

    #[inline(always)]
    pub const fn keys_len(&self) -> usize {
        unsafe {
            // ptr::read_unaligned(self.key_array.as_ptr() as *const ObjectCount) as usize
            *(self.key_array.as_ptr() as *const ObjectCount) as usize
        }
    }

    #[inline(always)]
    pub const fn len(&self) -> usize {
        self.keys_len()
    }

    #[inline(always)]
    pub const fn is_empty(&self) -> bool {
        self.keys_len() == 0
    }

    #[inline(always)]
    pub const fn is_full(&self) -> bool {
        self.children_len() == FAN_OUT
    }

    #[inline(always)]
    pub const fn children_len(&self) -> usize {
        match self.keys_len() {
            0 => 0,
            n => n + 1
        }
    }

    #[inline(always)]
    pub const fn keys(&self) -> &[Key] {
        unsafe {
            std::slice::from_raw_parts(self.key_array.as_ptr().add(1) as _, self.keys_len())
        }
    }

    #[inline(always)]
    pub const fn children(&self) -> &[BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>] {
        unsafe {
            std::slice::from_raw_parts(self.children_array.as_ptr() as _, self.children_len())
        }
    }
}