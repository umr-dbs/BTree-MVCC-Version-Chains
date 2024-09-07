use std::hash::Hash;
use std::mem;
use std::fmt::{Display, Formatter};
use std::ops::{Deref, DerefMut};
use crate::record_model::unsafe_clone::UnsafeClone;
use crate::record_model::Version;
use crate::utils::shadow_vec::VersionList;

#[derive(Default)]
pub(crate) struct VersionedRecordPoint<Key: Ord + Copy + Hash + Default, Payload: Clone + Default> {
    pub key: Key,
    pub version_list: VersionList<Payload>
}

impl<Key: Ord + Copy + Hash + Default, Payload: Clone + Default> Deref for VersionedRecordPoint<Key, Payload> {
    type Target = VersionList<Payload>;
    fn deref(&self) -> &Self::Target {
        &self.version_list
    }
}

impl<Key: Ord + Copy + Hash + Default, Payload: Clone + Default> DerefMut for VersionedRecordPoint<Key, Payload> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.version_list
    }
}


impl<Key: Ord + Copy + Hash + Default, Payload: Clone + Default> Clone for VersionedRecordPoint<Key, Payload> {
    #[inline(always)]
    fn clone(&self) -> Self {
        Self {
            key: self.key(),
            version_list: self.version_list().clone(),
        }
    }
}

impl<Key: Ord + Copy + Hash + Default, Payload: Clone + Default> VersionedRecordPoint<Key, Payload> {
    #[inline(always)]
    pub const fn new(key: Key, version: Version, payload: Payload) -> Self {
        Self {
            key,
            version_list: VersionList::new(version, payload)
        }
    }

    #[inline(always)]
    pub const fn key(&self) -> Key {
        self.key
    }

    #[inline(always)]
    pub const fn key_ref(&self) -> &Key {
        &self.key
    }

    #[inline(always)]
    pub const fn version_list(&self) -> &VersionList<Payload> {
        &self.version_list
    }

    #[inline(always)]
    pub fn version_list_mut(&mut self) -> &mut VersionList<Payload> {
        &mut self.version_list
    }
}

impl<Key: Ord + Copy + Hash + Default, Payload: Clone + Default> UnsafeClone for VersionedRecordPoint<Key, Payload> {
    #[inline(always)]
    unsafe fn unsafe_clone(&self) -> Self {
        mem::transmute_copy(self)
    }
}

impl<Key: Display + Ord + Copy + Hash + Default, Payload: Default + Display + Clone> Display for VersionedRecordPoint<Key, Payload> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecordPoint(Key: {}, VersionList-Len: {})", self.key(), self.version_list().len())
    }
}

