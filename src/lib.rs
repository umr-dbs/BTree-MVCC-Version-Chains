pub mod block;
pub mod crud_model;
pub mod locking;
pub mod page_model;
pub mod record_model;
pub mod tree;
pub mod utils;
pub mod n_test;
pub mod version_index;
type BTreeApi = INDEX;

#[allow(non_camel_case_types)]
#[repr(C)]
pub struct tree_options_t {
    key_size: libc::size_t,
    value_size: libc::size_t,
    pool_path: CString,
    pool_size: libc::size_t,
    num_threads: libc::size_t,
}

impl Default for tree_options_t {
    fn default() -> Self {
        Self {
            key_size: 8,
            value_size: 8,
            pool_path: CString::new("").unwrap(),
            pool_size: 0,
            num_threads: 1,
        }
    }
}

struct BTreeApiExport(BTreeApi);

impl Deref for BTreeApiExport {
    type Target = BTreeApi;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

use std::ffi::{c_int, c_void, CString};
use std::{mem, ptr};
use std::ops::Deref;
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::crud_model::crud_api::CRUDDispatcher;
use crate::locking::locking_strategy::{hybrid_lock_attempts, LHL_read_write, LockingStrategy, orwc, orwc_attempts};
use crate::n_test::INDEX;
use crate::record_model::v_record_point::VersionIndexType;
use crate::record_model::v_record_point::VersionIndexType::VANILLA;
use crate::tree::bplus_tree::new_INDEX;
use crate::utils::interval::Interval;

impl BTreeApiExport {
    #[inline(always)]
    fn find(&self, key: *const u8, _sz: usize, value_out: *mut u8) -> bool {
        match self.dispatch(CRUDOperation::Point(
            unsafe { ptr::read(mem::transmute(key)) }, self.committed_version()))
        {
            (.., CRUDOperationResult::MatchedRecord(Some(result)))
             => unsafe {
                ptr::write(mem::transmute(value_out), result.payload);
                true
            },
            _ => false
        }
    }

    #[inline(always)]
    fn insert(&self, key: *const u8, _key_sz: usize, value: *const u8, _value_sz: usize) -> bool {
        match self.dispatch(CRUDOperation::Insert(
            unsafe { ptr::read(mem::transmute(key)) },
            unsafe { ptr::read(mem::transmute(value)) }))
        {
            (.., CRUDOperationResult::Inserted(..)) => true,
            _ => {
                // let (key_n, value_n): (u64, u64)  = (unsafe { ptr::read(mem::transmute(key)) },
                //                                      unsafe { ptr::read(mem::transmute(value)) });
                // print!("Locking Strategy: {}", self.locking_strategy());
                // 
                // println!("{e}: Key: {key_n}, value: {value_n}");
                // let mut bo = Box::new(0u64);
                // let find = self.find(key, 8, bo.as_mut() as *mut _ as *mut _);
                // 
                // println!("Iss Duplicated: {find}");
                // 
                // let (_, c) 
                //     = self.dispatch(CRUDOperation::Update(key_n, value_n));
                // 
                // println!("Attempt to Update: {c}");

                // false
                true
            }
        }
    }

    #[inline(always)]
    fn update(&self, key: *const u8, _key_sz: usize, value: *const u8, _value_sz: usize) -> bool {
        match self.dispatch(CRUDOperation::Update(
            unsafe { ptr::read(mem::transmute(key)) },
            unsafe { ptr::read(mem::transmute(value)) }))
        {
            (.., CRUDOperationResult::Updated(..)) => true,
            _ => false
        }
    }

    #[inline(always)]
    fn remove(&self, key: *const u8, _key_sz: usize) -> bool {
        match self.dispatch(CRUDOperation::Delete(
            unsafe { ptr::read(mem::transmute(key)) }))
        {
            (.., CRUDOperationResult::Deleted(..)) => true,
            _ => false
        }
    }

    #[inline(always)]
    fn scan(&self, key: *const u8, _key_sz: usize, mut scan_sz: i32, mut values_out: *mut *mut u8) -> i32 {
        let key_start = unsafe { *(key as *const u64) };
        let key_end = key_start + scan_sz as u64 - 1;
        let mut len = 0;

        match self.dispatch(CRUDOperation::Range(Interval::new(key_start, key_end), self.committed_version())) {
            (.., CRUDOperationResult::MatchedRecords(mut buff)) if !buff.is_empty() => unsafe {
                buff.shrink_to_fit();

                *values_out = buff.as_mut_ptr() as _;
                len = buff.len() as _;

                mem::forget(buff);
            }
            _ => {}
        }

        len
    }
}

pub const ORWC: c_int = 0;
pub const OLC: c_int = 1;
pub const LHL: c_int = 2;
pub const MONO: c_int = 3;
pub const HL: c_int = 4;
pub const LC: c_int = 5;

pub const V_INDEX_KIND: VersionIndexType = VANILLA;
#[no_mangle]
pub extern "C" fn init_tree(p: c_int, e1: c_int, e2: c_int) -> *mut c_void {
    let lp = match p {
        ORWC => orwc_attempts(e1 as _),
        OLC => LockingStrategy::OLC,
        LHL => LHL_read_write(e1 as _, e2 as _),
        MONO => LockingStrategy::MonoWriter,
        HL => hybrid_lock_attempts(e1 as _),
        LC => LockingStrategy::LockCoupling,
        _ => orwc(),
    };
    
    Box::into_raw(Box::new(BTreeApiExport(new_INDEX(lp, V_INDEX_KIND)))) as _
}

#[no_mangle]
pub extern "C" fn destroy_tree_api(
    api: *mut c_void)
{
    if !api.is_null() {
        unsafe {
            let _tree = Box::from_raw(api as *mut BTreeApiExport);
        }
    }
}

#[no_mangle]
pub extern "C" fn tree_api_find(
    api: *mut c_void,
    key: *const u8,
    sz: usize,
    value_out: *mut u8) -> bool
{
    let api = unsafe { &*(api as *mut BTreeApiExport) };
    api.find(key, sz, value_out)
}

#[no_mangle]
pub extern "C" fn tree_api_insert(
    api: *mut c_void,
    key: *const u8,
    key_sz: usize,
    value: *const u8,
    value_sz: usize) -> bool
{
    let api = unsafe { &*(api as *mut BTreeApiExport) };
    api.insert(key, key_sz, value, value_sz)
}

#[no_mangle]
pub extern "C" fn tree_api_update(
    api: *mut c_void,
    key: *const u8,
    key_sz: usize,
    value: *const u8,
    value_sz: usize) -> bool
{
    let api = unsafe { &*(api as *mut BTreeApiExport) };
    api.update(key, key_sz, value, value_sz)
}

#[no_mangle]
pub extern "C" fn tree_api_remove(
    api: *mut c_void,
    key: *const u8,
    key_sz: usize) -> bool
{
    let api = unsafe { &*(api as *mut BTreeApiExport) };
    api.remove(key, key_sz)
}

#[no_mangle]
pub extern "C" fn tree_api_scan(
    api: *mut c_void,
    key: *const u8,
    key_sz: usize,
    scan_sz: i32,
    values_out: *mut *mut u8) -> i32
{
    let api = unsafe { &*(api as *mut BTreeApiExport) };
    api.scan(key, key_sz, scan_sz, values_out)
}


