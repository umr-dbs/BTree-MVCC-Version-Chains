use std::cell::Cell;
use std::fmt::Display;
use std::sync::Arc;
use arc_swap::ArcSwap;
use crate::record_model::Version;

// TODO: adjust ptrs via ArcSwap like weaver for efficient shallow thread-safe clone
type VanillaNodeLink<Payload> = Option<Arc<VersionedEntry<Payload>>>;

#[derive(Default, Clone)]
pub struct VersionedEntry<Payload: Clone + Default + Display + Send + Sync + 'static> {
    pub(crate) next: VanillaNodeLink<Payload>,
    pub payload: Payload,
    pub insert_version: Version,
    pub del_version: Cell<Version>,
}

#[derive(Default)]
pub struct AtomicVersionList<Payload: Clone + Default + Display + Sync + Send + 'static> {
    head: ArcSwap<VersionedEntry<Payload>>,
}

pub struct VersionListIterator<Payload: Clone + Default + Display + Sync + Send + 'static> {
    current: VanillaNodeLink<Payload>,
}

impl<Payload: Clone + Default + Display + Sync + Send + 'static> Iterator for VersionListIterator<Payload> {
    type Item = Arc<VersionedEntry<Payload>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current.take() {
            Some(version_entry) => {
                self.current = match version_entry.next.as_ref() {
                    Some(next) => Some(next.clone()),
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
        Self {
            head: ArcSwap::new(self.head.load_full())
        }
    }
}

impl<Payload: Clone + Default + Display + Sync + Send + 'static> AtomicVersionList<Payload> {
    #[inline(always)]
    pub fn iter(&self) -> VersionListIterator<Payload> {
        VersionListIterator {
            current: Some(self.head.load_full())
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    #[inline(always)]
    pub fn new(payload: Payload, insert_version: Version, del_version: Option<Version>) -> Self {
        Self {
            head: ArcSwap::new(Arc::new(VersionedEntry {
                next: None,
                payload,
                insert_version,
                del_version: Cell::new(del_version.unwrap_or(Version::MAX))
            }))
        }
    }

    #[inline]
    pub fn find(&self, version: Version) -> VanillaNodeLink<Payload> {
        let mut curr
            = self.head.load_full();

        loop {
            if curr.insert_version <= version && curr.del_version.get() > version {
                return Some(curr);
            }

            curr = match curr.next.as_ref() {
                None => break,
                Some(next) => next.clone(),
            };
        }

        None
    }

    #[inline(always)]
    unsafe fn insert_entry(&self, insert_version: Version, del_version: Version, payload: Payload) {
        self.head.store(Arc::new(VersionedEntry {
            next: Some(self.head.load_full()),
            payload,
            insert_version,
            del_version: Cell::new(del_version),
        }));
    }

    #[inline]
    pub fn push(&self, insert_version: Version, payload: Payload) {
        unsafe {
            self.insert_entry(
                insert_version,
                Version::MAX,
                payload);
        }
    }

    #[inline]
    pub fn append(&self, insert_version: Version, payload: Payload) -> Payload {
        let head
            = self.head.load_full();

        let old_ele = head.payload.clone();

        let new_head = Arc::new(VersionedEntry {
            next: Some(head.clone()),
            payload,
            insert_version,
            del_version: Cell::new(Version::MAX),
        });

        if head.del_version.get() == Version::MAX {
            head.del_version.set(insert_version);
        }

        self.head.store(new_head);

        old_ele
    }

    #[inline]
    pub fn delete(&self, del_version: Version) -> Option<Payload> {
        let head
            = self.head.load_full();

        if head.insert_version < del_version && head.del_version.get() == Version::MAX {
            head.del_version.set(del_version);

            Some(head.payload.clone())
        } else {
            None
        }
    }

    #[inline(always)]
    pub fn is_live(&self) -> bool {
        self.head.load().del_version.get() == Version::MAX
    }

    #[inline(always)]
    pub fn newest_payload(&self) -> Payload {
        self.head.load().payload.clone()
    }
}