use std::hash::Hash;
use std::mem;
use std::fmt::{Display, Formatter};
use std::ops::{Deref, DerefMut};
use serde::{Deserialize, Serialize};
use crate::record_model::unsafe_clone::UnsafeClone;
use crate::record_model::Version;
use crate::utils::shadow_vec::{VersionIndex, VersionList};


#[derive(Clone, Copy, Serialize, Deserialize)]
pub enum VersionIndexType {
    VANILLA,
    SkipList,
    SkipListSynced,
    BTree
}

impl Display for VersionIndexType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            VersionIndexType::VANILLA => write!(f, "VANILLA"),
            VersionIndexType::SkipList => write!(f, "SkipList"),
            VersionIndexType::SkipListSynced => write!(f, "SkipListSynced"),
            VersionIndexType::BTree => write!(f, "BTree"),
        }
    }
}

pub(crate) struct VersionedRecordPoint<
    Key: Ord + Copy + Hash + Default,
    Payload: Default + Clone + Send + Sync + Display + 'static>
{
    pub key: Key,
    pub version_index: VersionIndex<Payload>
}


impl<Key: Ord + Copy + Hash + Default,
    Payload: Default + Clone + Send + Sync + Display + 'static>  Clone for VersionedRecordPoint<Key, Payload>
{
    fn clone(&self) -> Self {
        Self {
            key: self.key,
            version_index: self.version_index.clone(),
        }
    }
}
impl<Key: Ord + Copy + Hash + Default,
    Payload: Default + Clone + Send + Sync + Display + 'static>  Default for VersionedRecordPoint<Key, Payload>
{
    fn default() -> Self {
        Self {
            key: Key::default(),
            version_index: VersionIndex::VANILLA(VersionList::default()),
        }
    }
}

impl<Key: Ord + Copy + Hash + Default, Payload: Default + Clone + Send + Sync + Display + 'static> Deref for VersionedRecordPoint<Key, Payload> {
    type Target = VersionIndex<Payload>;
    fn deref(&self) -> &Self::Target {
        &self.version_index
    }
}

impl<Key: Ord + Copy + Hash + Default, Payload: Default + Clone + Send + Sync + Display + 'static> DerefMut for VersionedRecordPoint<Key, Payload> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.version_index
    }
}


// impl<Key: Ord + Copy + Hash + Default, Payload: Default + Clone + Send + Sync + Display + 'static> Clone for VersionedRecordPoint<Key, Payload> {
//     #[inline(always)]
//     fn clone(&self) -> Self {
//         Self {
//             key: self.key(),
//             version_index: self.version_index().clone(),
//         }
//     }
// }

impl<Key: Ord + Copy + Hash + Default, Payload: Default + Clone + Send + Sync + Display + 'static> VersionedRecordPoint<Key, Payload> {
    #[inline(always)]
    pub fn new(key: Key, version: Version, payload: Payload, kind: VersionIndexType) -> Self {
        Self {
            key,
            version_index: VersionIndex::new(kind, payload, version)
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
    pub const fn version_index(&self) -> &VersionIndex<Payload> {
        &self.version_index
    }

    #[inline(always)]
    pub fn version_index_mut(&mut self) -> &mut VersionIndex<Payload> {
        &mut self.version_index
    }
}

// impl<Key: Ord + Copy + Hash + Default, Payload: Clone + Default> UnsafeClone for VersionedRecordPoint<Key, Payload> {
//     #[inline(always)]
//     unsafe fn unsafe_clone(&self) -> Self {
//         mem::transmute_copy(self)
//     }
// }

impl<Key: Display + Ord + Copy + Hash + Default,
    Payload: Default + Clone + Send + Sync + Display + 'static> Display for VersionedRecordPoint<Key, Payload>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecordPoint(Key: {}, VersionList-Len: {})", self.key(), self.version_index().len())
    }
}

