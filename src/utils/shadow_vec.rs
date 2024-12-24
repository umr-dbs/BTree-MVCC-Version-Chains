use std::cell::Cell;
use std::{mem, ptr, slice};
use std::ops::{Deref, IndexMut};
use std::ptr::slice_from_raw_parts_mut;
use std::sync::Arc;
use crate::page_model::ObjectCount;
use crate::record_model::Version;
use crate::utils::safe_cell::SafeCell;

pub struct ShadowVec<E: Default + Clone> {
    pub(crate) ptr: *mut E,
    pub(crate) len: SafeCell<usize>,
    pub(crate) update_len: Option<*mut ObjectCount>,
}

impl<E: Default + Clone> ShadowVec<E> {
    pub fn get_unchecked_mut(&self, index: usize) -> &mut E {
        unsafe {
            &mut *self.ptr.add(index)
        }
    }

    pub fn get_unchecked(&self, index: usize) -> &E {
        unsafe {
            &*self.ptr.add(index)
        }
    }

    pub fn clear(&self) {
        unsafe {
            (&mut *slice_from_raw_parts_mut(self.ptr, *self.len))
                .iter_mut()
                .for_each(|c| ptr::drop_in_place(c));

            *self.len.get_mut() = 0;
        }
    }

    pub fn extend<I>(&self, items: I) where I: IntoIterator<Item=E> {
        let mut len
            = *self.len;

        items.into_iter().for_each(|item| unsafe {
            self.ptr
                .add(len)
                .write(item);

            len += 1;
        });

        *self.len.get_mut() = len
    }

    pub fn pop(&self) -> E {
        let len
            = *self.len;

        *self.len.get_mut() = len - 1;

        unsafe {
            self.ptr
                .add(len - 1)
                .read()
        }
    }

    pub fn remove(&self, index: usize) -> E {
        unsafe {
            let len
                = *self.len;

            if index == len - 1 {
                return self.pop();
            }

            let e = self
                .ptr
                .add(index)
                .read();

            self.ptr
                .add(index)
                .copy_from(
                    self.ptr.add(index + 1),
                    len - index - 1);

            *self.len.get_mut() = len - 1;

            e
        }
    }

    pub fn push(&self, e: E) {
        unsafe {
            let len
                = *self.len;

            self.ptr
                .add(len)
                .write(e);

            *self.len.get_mut() = len + 1
        }
    }

    pub fn insert(&self, index: usize, e: E) {
        unsafe {
            let len
                = *self.len;

            let p
                = self.ptr.add(index);

            if index < len {
                ptr::copy(p, p.add(1), len - index);
            }

            p.write(e);

            *self.len.get_mut() = len + 1
        }
    }

    pub fn extend_from_slice(&self, other: &[E]) {
        unsafe {
            let len
                = *self.len;

            let p = self.ptr.add(len);
            other.iter()
                .enumerate()
                .for_each(|(i, e)| p.add(i).write(e.clone()));
            
            // ptr::copy(other.as_ptr(), self.ptr.add(len), other.len());

            *self.len.get_mut() = len + other.len()
        }
    }
}

impl<E: Default + Clone> Drop for ShadowVec<E> {
    fn drop(&mut self) {
        unsafe {
            if let Some(obj_len_ptr) = self.update_len {
                ptr::write_unaligned(obj_len_ptr, *self.len as _)
                // *self.obj_cnt = self.unreal_vec.len() as _
            }
        }
    }
}

#[derive(Clone, Default)]
pub struct VersionList<Payload: Clone + Default>(VEntryPayload<Payload>);

unsafe impl<Payload: Clone + Default> Sync for VersionList<Payload> {}

impl<Payload: Clone + Default> Deref for VersionList<Payload> {
    type Target = VEntryPayload<Payload>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<Payload: Clone + Default> VersionList<Payload> {
    #[inline(always)]
    pub const fn new(version: Version, payload: Payload) -> Self {
        Self(VEntryPayload {
            entry: VTuple {
                version,
                del_version: Version::MAX,
                payload,
            },
            next: None
        })
    }

    #[inline(always)]
    pub fn is_live(&self) -> bool {
        self.del_version == Version::MAX
    }

    #[inline(always)]
    pub fn newest_payload(&self) -> Payload {
        self.0.entry.payload.clone()
    }

    #[inline(always)]
    pub fn newest_version(&self) -> Version {
        self.0.entry.version
    }

    pub fn oldest_payload(&self) -> Payload {
        let mut curr
            = &self.0;

        while let Some(ref next) = curr.next {
            curr = next;
        }

        curr.entry.payload.clone()
    }

    pub fn find(&self, version: Version) -> Option<&VEntryPayload<Payload>> {
        let mut curr
            = &self.0;

        while curr.entry.version > version {
            curr = curr.next.as_deref()?;
        }

        if curr.entry.del_version > version {
            Some(&curr)
        }
        else {
            None
        }
    }

    #[inline(always)]
    pub fn delete(&mut self, del_version: Version) -> Option<Payload> {
        self.delete_internal(self.newest_version(), del_version)
    }

    #[inline(always)]
    fn delete_internal(&mut self, version: Version, del_version: Version) -> Option<Payload> {
        if self.entry.version > version && self.entry.del_version > del_version {
            self.0.entry.del_version = del_version;
            Some(self.entry.payload.clone())
        }
        else {
            None
        }
    }

    pub fn find_mut(&mut self, version: Version) -> Option<&mut VEntryPayload<Payload>> {
        let mut curr
            = &mut self.0;

        while curr.entry.version > version {
            curr = curr.next.as_deref()?.get_mut();
        }

        if curr.entry.del_version > version {
            Some(curr)
        }
        else {
            None
        }
    }

    pub fn append(&mut self, version: Version, payload: Payload) -> Payload {
        let old_self = Arc::new(SafeCell::new(mem::replace(&mut self.0, VEntryPayload {
            entry: VTuple {
                version,
                del_version: Version::MAX,
                payload,
            },
            next: None
        })));

        let old_payload = old_self
            .payload()
            .clone();

        self.0.next = Some(old_self);

        old_payload
    }

    pub fn len(&self) -> usize {
        let mut len
            = 1;

        let mut curr
            = self.0.next.as_deref();

        while let Some(next) = curr {
            curr = next.next.as_deref();
            len += 1;
        }

        len
    }
}

#[derive(Clone, Default)]
pub struct VEntryPayload<Payload: Clone + Default>  {
    pub entry: VTuple<Payload>,
    next: Option<Arc<SafeCell<VEntryPayload<Payload>>>>
}

impl<Payload: Clone + Default> Deref for VEntryPayload<Payload> {
    type Target = VTuple<Payload>;
    fn deref(&self) -> &Self::Target {
        &self.entry
    }
}

#[derive(Clone, Default)]
pub struct VTuple<Payload: Clone + Default> {
    version: Version,
    pub del_version: Version,
    payload: Payload
}

impl<Payload: Clone + Default>  VTuple<Payload> {
    pub fn payload(&self) -> &Payload {
        &self.payload
    }
}
impl<Payload: Clone + Default> Deref for VTuple<Payload> {
    type Target = Payload;
    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}

const DEL_VERSION_FLAG: Version = 1 << 63;