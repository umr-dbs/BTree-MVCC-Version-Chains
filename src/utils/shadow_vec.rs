use std::cell::Cell;
use std::{mem, ptr, slice};
use std::collections::LinkedList;
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
pub struct VersionList<Payload: Clone + Default>(LinkedList<VTuple<Payload>>);

unsafe impl<Payload: Clone + Default> Sync for VersionList<Payload> {}


impl<Payload: Clone + Default> VersionList<Payload> {
    #[inline(always)]
    pub fn new(version: Version, payload: Payload) -> Self {
        Self(LinkedList::from([VTuple {
            version,
            del_version: Version::MAX,
            payload,
        }]))
    }

    #[inline(always)]
    pub fn is_live(&self) -> bool {
        self.0
            .front()
            .map(|l| l.del_version == Version::MAX)
            .unwrap_or(false)
    }

    #[inline(always)]
    pub fn newest_payload(&self) -> Payload {
        self.0
            .front()
            .map(|l| l.payload.clone())
            .unwrap_or(Payload::default())
    }

    #[inline(always)]
    pub fn newest_version(&self) -> Version {
        self.0
            .front()
            .map(|l| l.version)
            .unwrap_or(Version::MIN)
    }

    pub fn oldest_payload(&self) -> VTuple<Payload> {
        let mut curr
            = &self.0;

        curr.back().unwrap().clone()
    }

    pub fn find(&self, version: Version) -> Option<&VTuple<Payload>> {
        self.0.iter()
           .find(|v| v.version < version)
           .filter(|v| v.del_version > version)
    }

    #[inline(always)]
    pub fn delete(&mut self, del_version: Version) -> Option<Payload> {
        self.delete_internal(self.newest_version(), del_version)
    }

    #[inline(always)]
    fn delete_internal(&mut self, version: Version, del_version: Version) -> Option<Payload> {
        let item
            = self.0.front_mut().unwrap();

        if item.version > version && item.del_version > del_version {
            item.del_version = del_version;
            Some(item.payload.clone())
        }
        else {
            None
        }
    }

    pub fn find_mut(&mut self, version: Version) -> Option<&mut VTuple<Payload>> {
        self.0
            .iter_mut()
            .find(|v| v.version < version && v.del_version > version)
    }

    pub fn append(&mut self, version: Version, payload: Payload) -> Payload {
        let old = self.0.front().unwrap().payload.clone();
        self.0.push_front(VTuple {
            version,
            del_version: Version::MAX,
            payload,
        });

        old
    }

    pub fn len(&self) -> usize {
        self.0.len()
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