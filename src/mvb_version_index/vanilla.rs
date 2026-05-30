use std::cell::Cell;
use std::fmt::Display;
use std::ptr;
use std::ptr::null;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::{Acquire, Release};
use itertools::Itertools;
use crate::mvb_record_model::Version;

#[derive(Default, Clone)]
pub struct VersionedEntry<Payload: Clone + Default + Display + Send + Sync + 'static> {
    pub(crate) next: *const VersionedEntry<Payload>,
    pub payload: Payload,
    pub insert_version: Version,
    pub del_version: Cell<Version>,
}

#[derive(Default)]
pub struct AtomicVersionList<Payload: Clone + Default + Display + Sync + Send + 'static> {
    head: AtomicPtr<VersionedEntry<Payload>>,
}

impl<Payload: Clone + Default + Display + Sync + Send + 'static> Drop for AtomicVersionList<Payload> {
    fn drop(&mut self) {
        let mut curr
            = self.head.load(Acquire);

        while !curr.is_null() {
            unsafe {
                let boxed = Box::from_raw(curr);
                curr = boxed.next as *mut _;
            }
        }
    }
}

pub struct VersionListIterator<Payload: Clone + Default + Display + Sync + Send + 'static> {
    current: Option<*const VersionedEntry<Payload>>,
}

impl<Payload: Clone + Default + Display + Sync + Send + 'static> Iterator for VersionListIterator<Payload> {
    type Item = (Payload, Version, Version);

    fn next(&mut self) -> Option<Self::Item> {
        match self.current.take() {
            Some(version_entry) => unsafe {
                let curr = &*version_entry;

                self.current = match curr.next.is_null() {
                    false => Some(curr.next),
                    _ => None,
                };

                Some((curr.payload.clone(), curr.insert_version, curr.del_version.get()))
            }
            _ => None,
        }
    }
}

impl<Payload: Clone + Default + Display + Sync + Send + 'static> Clone for AtomicVersionList<Payload> {
    fn clone(&self) -> Self {
        let mut iter
            = self.iter().collect_vec();

        let last = iter.pop().unwrap();
        let list = Self::new(
            last.0, last.1, if last.2 == Version::MAX { None } else { Some(last.2) });

        iter.into_iter().rev().for_each(|(p, i, _)|
            list.append(i, p));

        list
    }
}

impl<Payload: Clone + Default + Display + Sync + Send + 'static> AtomicVersionList<Payload> {
    #[inline(always)]
    pub fn iter(&self) -> VersionListIterator<Payload> {
        VersionListIterator {
            current: Some(self.head.load(Acquire))
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    #[inline(always)]
    pub fn new(payload: Payload, insert_version: Version, del_version: Option<Version>) -> Self {
        Self {
            head: AtomicPtr::new(Box::into_raw(Box::new(VersionedEntry {
                next: null(),
                payload,
                insert_version,
                del_version: Cell::new(del_version.unwrap_or(Version::MAX))
            })))
        }
    }

    #[inline]
    pub fn find(&self, version: Version) -> Option<&VersionedEntry<Payload>> {
        let mut curr
            = unsafe { &*self.head.load(Acquire) };

        loop {
            if curr.insert_version <= version && curr.del_version.get() > version {
                return Some(curr);
            }

            curr = match unsafe { curr.next.as_ref() } {
                None => break,
                Some(next) => next,
            };
        }

        None
    }

    #[inline]
    pub fn push(&self, insert_version: Version, payload: Payload) {
        self.append(insert_version, payload);
    }

    #[inline]
    pub fn append(&self, insert_version: Version, payload: Payload) {
        let head
            = unsafe { &*self.head.load(Acquire) };

        let new_head = Box::into_raw(Box::new(VersionedEntry {
            next: head,
            payload,
            insert_version,
            del_version: Cell::new(Version::MAX),
        }));

        if head.del_version.get() == Version::MAX {
            head.del_version.set(insert_version);
        }

        self.head.store(new_head, Release);
    }

    #[inline]
    pub fn delete(&self, del_version: Version) {
        let head
            = unsafe { &*self.head.load(Acquire) };

        if head.insert_version < del_version && head.del_version.get() == Version::MAX {
            head.del_version.set(del_version);
        }
    }

    #[inline(always)]
    pub fn is_live(&self) -> bool {
        unsafe { &*self.head.load(Acquire) }.del_version.get() == Version::MAX
    }

    #[inline(always)]
    pub fn newest_payload(&self) -> Payload {
        unsafe { &*self.head.load(Acquire) }.payload.clone()
    }
}