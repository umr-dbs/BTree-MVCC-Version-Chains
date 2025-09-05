use std::fmt::Display;
use std::ptr::null_mut;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::{Acquire, Release};
use itertools::Itertools;
use crate::record_model::Version;

// TODO: adjust ptrs via ArcSwap like weaver for efficient shallow thread-safe clone
#[derive(Default, Clone)]
pub struct VersionedEntry<Payload: Clone + Default + Display + Send + Sync + 'static> {
    pub(crate) next: Option<*mut VersionedEntry<Payload>>,
    pub payload: Payload,
    pub insert_version: Version,
    pub del_version: Version,
}

#[derive(Default)]
pub struct AtomicVersionList<Payload: Clone + Default + Display + Sync + Send + 'static> {
    head: AtomicPtr<VersionedEntry<Payload>>,
    // len: AtomicUsize
}

pub struct VersionListIterator<Payload: Clone + Default + Display + Sync + Send + 'static> {
    current: Option<VersionedEntry<Payload>>,
}

impl<Payload: Clone + Default + Display + Sync + Send + 'static> Iterator for VersionListIterator<Payload> {
    type Item = VersionedEntry<Payload>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current.take() {
            Some(version_entry) => {
                self.current = match version_entry.next {
                    Some(next) => unsafe { Some((*next).clone()) },
                    _ => None,
                };

                Some(version_entry)
            }
            _ => None,
        }
    }
}

impl<Payload: Clone + Default + Display + Sync + Send + 'static> Clone for AtomicVersionList<Payload> {
    fn clone(&self) -> Self {
        let data = self.iter().collect_vec();
        let list = AtomicVersionList {
            head: AtomicPtr::new(null_mut())
        };

        data.into_iter().rev().for_each(|entry| unsafe {
            list.insert_entry(entry.insert_version, entry.del_version, entry.payload);
        });

        list
    }
}

impl<Payload: Clone + Default + Sync + Send + Display + 'static> Drop for AtomicVersionList<Payload> {
    fn drop(&mut self) {
        unsafe {
            let mut curr = self.head.load(Acquire);

            // fence(Acquire);
            while !curr.is_null() {
                let mut curr_ref = Box::from_raw(curr);

                curr = curr_ref.next.take().unwrap_or(null_mut());

                drop(curr_ref);
            }
        }
    }
}

impl<Payload: Clone + Default + Display + Sync + Send + 'static> AtomicVersionList<Payload> {
    #[inline(always)]
    pub fn iter(&self) -> VersionListIterator<Payload> {
        let ptr = self.head.load(Acquire);

        VersionListIterator {
            current: match ptr.is_null() {
                true => None,
                _ => unsafe {
                    // fence(Acquire);

                    Some((*ptr).clone())
                },
            },
        }
    }

    #[inline(always)]
    fn head_ref(&self) -> &VersionedEntry<Payload> {
        let p = unsafe { &*self.head.load(Acquire) };

        // fence(Acquire);
        p
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.iter().count()
        // self.len.load(Acquire)
    }

    // #[inline(always)]
    // pub fn from(insert_version: Version, payload: Payload) -> Self {
    //     Self::new(payload, insert_version)
    // }

    #[inline(always)]
    pub fn new(payload: Payload, insert_version: Version, del_version: Option<Version>) -> Self {
        Self {
            head: AtomicPtr::new(Box::into_raw(Box::new(VersionedEntry {
                next: None,
                payload,
                insert_version,
                del_version: del_version.unwrap_or(Version::MAX),
            }))),
            // len: AtomicUsize::new(1)
        }
    }

    #[inline]
    pub fn find(&self, version: Version) -> Option<&VersionedEntry<Payload>> {
        let mut curr = self.head_ref();

        loop {
            if curr.insert_version <= version && curr.del_version > version {
                return Some(curr);
            }

            curr = match curr.next {
                None => break,
                Some(next) => unsafe { &*next },
            };
        }

        None
    }

    #[inline]
    unsafe fn insert_entry(&self, insert_version: Version, del_version: Version, payload: Payload) {
        let head_p
            = self.head.load(Acquire);

        let next
            = if head_p.is_null() { None } else { Some(head_p) };

        let new_head = Box::into_raw(Box::new(VersionedEntry {
            next,
            payload,
            insert_version,
            del_version,
        }));

        self.head.store(new_head, Release);
    }

    #[inline]
    pub fn push(&self, insert_version: Version, payload: Payload) {
        let new_head = Box::into_raw(Box::new(VersionedEntry {
            next: Some(self.head.load(Acquire)),
            payload,
            insert_version,
            del_version: Version::MAX,
        }));

        self.head.store(new_head, Release);
    }

    #[inline]
    pub fn append(&self, insert_version: Version, payload: Payload) -> Payload {
        let head_p
            = self.head.load(Acquire);

        let head
            = unsafe { &mut *head_p };

        let old_ele = head.payload.clone();

        let new_head = Box::into_raw(Box::new(VersionedEntry {
            next: Some(head_p),
            payload,
            insert_version,
            del_version: Version::MAX,
        }));

        if head.del_version == Version::MAX {
            head.del_version = insert_version;
        }

        self.head.store(new_head, Release);

        old_ele
    }

    #[inline]
    pub fn delete(&self, del_version: Version) -> Option<Payload> {
        let head_p
            = self.head.load(Acquire);

        let head
            = unsafe { &mut *head_p };

        if head.insert_version < del_version && head.del_version == Version::MAX {
            head.del_version = del_version;
            // fence(Release);

            Some(head.payload.clone())
        } else {
            None
        }
    }

    #[inline(always)]
    pub fn is_live(&self) -> bool {
        self.head_ref().del_version == Version::MAX
    }

    #[inline(always)]
    pub fn newest_payload(&self) -> Payload {
        self.head_ref().payload.clone()
    }
}