use std::hash::Hash;
use std::fmt::{Display, Formatter};
use std::ops::{Deref, DerefMut};
use serde::{Deserialize, Serialize};

use crate::record_model::Version;
use crate::record_model::version_info::DeletedVersionInfo;
use crate::version_index::vanilla::AtomicVersionList;
use crate::version_index::version_index::VersionIndex;

#[derive(Clone, Copy, Serialize, Deserialize)]
pub enum VersionIndexType {
    VWEAVER,
    VANILLA,
    SkipList,
    SkipListSynced,
    BTree
}

impl VersionIndexType {
    #[inline(always)]
    pub const fn is_skiplist(&self) -> bool {
        if let VersionIndexType::SkipList = self {
            true
        }
        else {
            false
        }
    }

    #[inline(always)]
    pub const fn is_skiplist_synced(&self) -> bool {
        if let VersionIndexType::SkipListSynced = self {
            true
        }
        else {
            false
        }
    }

    #[inline(always)]
    pub const fn is_v_weaver(&self) -> bool {
        if let VersionIndexType::VWEAVER = self {
            true
        }
        else {
            false
        }
    }
}

impl Display for VersionIndexType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            VersionIndexType::VANILLA => write!(f, "VANILLA"),
            VersionIndexType::SkipList => write!(f, "SkipList"),
            VersionIndexType::SkipListSynced => write!(f, "SkipListSynced"),
            VersionIndexType::BTree => write!(f, "BTree"),
            VersionIndexType::VWEAVER => write!(f, "vWeaver"),
        }
    }
}

pub(crate) struct VersionedRecordPoint<
    Key: Ord + Copy + Hash + Default,
    Payload: Default + Clone + Send + Sync + Display + 'static>
{
    pub key: Key,
    pub version_index: VersionIndex<Key, Payload>
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
            version_index: VersionIndex::VANILLA(AtomicVersionList::default()),
        }
    }
}

impl<Key: Ord + Copy + Hash + Default, Payload: Default + Clone + Send + Sync + Display + 'static> Deref for VersionedRecordPoint<Key, Payload> {
    type Target = VersionIndex<Key, Payload>;
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
            version_index: VersionIndex::new(key, kind, payload, version)
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
    pub const fn version_index(&self) -> &VersionIndex<Key, Payload> {
        &self.version_index
    }

    #[inline(always)]
    pub fn version_index_mut(&mut self) -> &mut VersionIndex<Key, Payload> {
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

#[derive(Clone, Default)]
pub struct RecordInfo<Payload: Clone + Display + Default + Sync + 'static> {
    pub del_version: DeletedVersionInfo,
    pub payload: Payload
}

impl<Payload: Clone + Display + Default + Sync + 'static> Display for RecordInfo<Payload> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Deleted version: {}, Payload: {}",
               self.del_version
                   .get()
                   .map(|del| del.to_string())
                   .unwrap_or("*".to_string()),
               self.payload)
    }
}

impl<Payload: Clone + Display + Default + Sync + 'static> RecordInfo<Payload> {
    pub const fn new(payload: Payload) -> RecordInfo<Payload> {
        Self {
            del_version: DeletedVersionInfo::new_null(),
            payload
        }
    }
}
