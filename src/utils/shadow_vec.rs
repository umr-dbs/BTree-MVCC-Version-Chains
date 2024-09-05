use std::cell::Cell;
use std::{ptr, slice};
use std::ptr::slice_from_raw_parts_mut;
use crate::page_model::ObjectCount;

pub struct ShadowVec<E: Default + Clone> {
    pub(crate) ptr: *mut E,
    pub(crate) len: Cell<usize>,
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
            (&mut *slice_from_raw_parts_mut(self.ptr, self.len.get()))
                .iter_mut()
                .for_each(|c| ptr::drop_in_place(c));

            self.len.set(0);
        }
    }

    pub fn extend<I>(&self, items: I) where I: IntoIterator<Item=E> {
        let mut len
            = self.len.get();

        items.into_iter().for_each(|item| unsafe {
            self.ptr
                .add(len)
                .write(item);

            len += 1;
        });

        self.len.set(len)
    }

    pub fn pop(&self) -> E {
        let len
            = self.len.get();

        self.len.set(len - 1);

        unsafe {
            self.ptr
                .add(len - 1)
                .read()
        }
    }

    pub fn remove(&self, index: usize) -> E {
        unsafe {
            let len
                = self.len.get();

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

            self.len.set(len - 1);

            e
        }
    }

    pub fn push(&self, e: E) {
        unsafe {
            let len
                = self.len.get();

            self.ptr
                .add(len)
                .write(e);

            self.len.set(len + 1)
        }
    }

    pub fn insert(&self, index: usize, e: E) {
        unsafe {
            let len
                = self.len.get();

            let p
                = self.ptr.add(index);
            
            if index < len {
                ptr::copy(p, p.add(1), len - index);
            }
            
            p.write(e);

            self.len.set(len + 1)
        }
    }

    pub fn extend_from_slice(&self, other: &[E]) {
        unsafe {
            let len
                = self.len.get();

            let p = self.ptr.add(len);
            other.iter()
                .enumerate()
                .for_each(|(i, e)| p.add(i).write(e.clone()));
            
            // ptr::copy(other.as_ptr(), self.ptr.add(len), other.len());

            self.len.set(len + other.len())
        }
    }
}

impl<E: Default + Clone> Drop for ShadowVec<E> {
    fn drop(&mut self) {
        unsafe {
            if let Some(obj_len_ptr) = self.update_len {
                ptr::write_unaligned(obj_len_ptr, self.len.get() as _)
                // *self.obj_cnt = self.unreal_vec.len() as _
            }
        }
    }
}