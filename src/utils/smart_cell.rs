use std::fmt::{Display, Formatter};
use std::{hint, mem, ptr};
use std::mem::{transmute, transmute_copy};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst};
use parking_lot::lock_api::{MutexGuard, RwLockReadGuard, RwLockWriteGuard};
use parking_lot::{Mutex, RawMutex, RawRwLock, RwLock};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use crate::page_model::Attempts;
use crate::record_model::{AtomicVersion, Version};
use crate::utils::safe_cell::SafeCell;
use crate::utils::smart_cell::SmartFlavor::{ExclusiveCell, FreeCell, HybridCell, LightWeightHybridCell, OLCCell, ReadersWriterCell};
use crate::utils::smart_cell::SmartGuard::{HybridRwReader, HybridRwWriter, LockFree, MutExclusive, OLCReader, OLCReaderPin, OLCWriter, RwReader, RwWriter};

pub const CPU_THREADS: bool = true;
pub const ENABLE_YIELD: bool = !CPU_THREADS;

pub(crate) const OBSOLETE_FLAG_VERSION: LatchVersion = 0x8_000000000000000;
const WRITE_FLAG_VERSION: LatchVersion = 0x4_000000000000000;
const PIN_FLAG_VERSION: LatchVersion = 0x2_000000000000000;
const ZEROED_FLAG_VERSION: LatchVersion = 0x0;

const WRITE_OBSOLETE_FLAG_VERSION: LatchVersion = 0xC_000000000000000;
const WRITE_PIN_FLAG_VERSION: LatchVersion = 0x6_000000000000000;
const WRITE_PIN_OBSOLETE_FLAG_VERSION: LatchVersion = 0xE_000000000000000;

#[cfg(all(feature = "olc-hle", any(target_arch = "x86", target_arch = "x86_64")))]
pub trait AtomicElisionExt {
    fn elision_compare_exchange_acquire(
        &self,
        current: Version,
        new: Version,
    ) -> Result<Version, Version>;
}

#[cfg(all(feature = "olc-hle", any(target_arch = "x86", target_arch = "x86_64")))]
impl AtomicElisionExt for AtomicVersion {
    #[inline(always)]
    fn elision_compare_exchange_acquire(&self, current: Version, new: Version) -> Result<Version, Version> {
        unsafe {
            use core::arch::asm;
            let prev: Version;
            #[cfg(target_pointer_width = "32")]
            asm!(
            "xacquire",
            "lock",
            "cmpxchg [{:e}], {:e}",
            in(reg) self,
            in(reg) new,
            inout("eax") current => prev,
            );
            #[cfg(target_pointer_width = "64")]
            asm!(
            "xacquire",
            "lock",
            "cmpxchg [{}], {}",
            in(reg) self,
            in(reg) new,
            inout("rax") current => prev,
            );
            if prev == current {
                Ok(prev)
            } else {
                Err(prev)
            }
        }
    }
}

#[repr(u8)]
#[derive(Copy, Clone)]
pub enum LatchType {
    Exclusive,
    ReadersWriter,
    Optimistic,
    Hybrid,
    LightWeightHybrid,
    None,
}
pub static mut COUNTERS: (AtomicUsize, AtomicUsize) =
    (AtomicUsize::new(0), AtomicUsize::new(0));

#[inline(always)]
#[cfg(target_os = "linux")]
pub fn sched_yield(attempt: Attempts) {
    if attempt > 3 {
        unsafe {
            COUNTERS.1.fetch_add(1, Relaxed);
            libc::sched_yield();
        }
    } else {
        unsafe { COUNTERS.0.fetch_add(1, Relaxed); }
        hint::spin_loop();
    }
}

pub const FORCE_YIELD: Attempts = 4;

#[inline(always)]
#[cfg(not(target_os = "linux"))]
pub fn sched_yield(attempt: usize) {
    if attempt > 3 {
        std::thread::yield_now();
    } else {
        hint::spin_loop();
    }
}

type LatchVersion = Version;
type IsRead = bool;

pub struct OptCell<E: Default> {
    pub(crate) cell: SafeCell<E>,
    pub(crate) cell_version: AtomicVersion,
}

impl<E: Default + Serialize> Serialize for OptCell<E> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        self.cell.serialize(serializer)
    }
}

impl<'de, E: Default + Deserialize<'de>> Deserialize<'de> for OptCell<E> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
        Ok(OptCell::new(E::deserialize(deserializer)?))
    }
}

impl<E: Default + Display> Display for OptCell<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "OptCell {{\ncell: {}\n\t\tcell_version: {}\n\t}}", self.cell.get_mut(), self.load_version())
    }
}

impl<E: Default> Default for OptCell<E> {
    fn default() -> Self {
        Self::new(E::default())
    }
}

impl<E: Default> OptCell<E> {
    const CELL_START_VERSION: LatchVersion = 0;

    #[inline(always)]
    pub const fn new(data: E) -> Self {
        Self {
            cell: SafeCell::new(data),
            cell_version: AtomicVersion::new(Self::CELL_START_VERSION),
        }
    }

    #[inline(always)]
    pub fn load_version(&self) -> LatchVersion {
        self.cell_version.load(Acquire)
    }

    #[inline(always)]
    pub fn read_lock(&self) -> (IsRead, LatchVersion) {
        let read_version
            = self.load_version();

        (read_version & WRITE_OBSOLETE_FLAG_VERSION == 0, read_version & !PIN_FLAG_VERSION)
    }

    #[cfg(all(feature = "olc-hle", any(target_arch = "x86", target_arch = "x86_64")))]
    #[inline(always)]
    pub fn pin_lock(&self) -> Result<LatchVersion, (IsRead, LatchVersion)> {
        let read_version
            = self.load_version();

        if read_version & PIN_FLAG_VERSION != 0 {
            return Err((true, read_version & !PIN_FLAG_VERSION));
        }

        if read_version & WRITE_OBSOLETE_FLAG_VERSION != 0 {
            return Err((false, read_version));
        }

        match self.cell_version.elision_compare_exchange_acquire(
            read_version,
            read_version | PIN_FLAG_VERSION)
        {
            Ok(_) => Ok(read_version | PIN_FLAG_VERSION),
            Err(_) => Err((true, read_version))
        }
    }

    #[cfg(not(all(feature = "olc-hle", any(target_arch = "x86", target_arch = "x86_64"))))]
    #[inline(always)]
    pub fn pin_lock(&self) -> Result<LatchVersion, (IsRead, LatchVersion)> {
        let read_version
            = self.load_version();

        if read_version & PIN_FLAG_VERSION != 0 {
            return Err((true, read_version & !PIN_FLAG_VERSION));
        }

        if read_version & WRITE_OBSOLETE_FLAG_VERSION != 0 {
            return Err((false, read_version));
        }

        match self.cell_version.compare_exchange_weak(
            read_version,
            read_version | PIN_FLAG_VERSION,
            AcqRel,
            Relaxed)
        {
            Ok(_) => Ok(read_version | PIN_FLAG_VERSION),
            Err(_) => Err((true, read_version))
        }
    }

    #[inline(always)]
    pub fn write_unpin(&self, pin_lock: LatchVersion) {
        debug_assert!(pin_lock & PIN_FLAG_VERSION == PIN_FLAG_VERSION &&
            pin_lock & WRITE_OBSOLETE_FLAG_VERSION == 0);

        self.cell_version.store(pin_lock ^ PIN_FLAG_VERSION, Relaxed)
    }

    #[inline(always)]
    pub fn is_read_valid(&self, read_latch: LatchVersion) -> IsRead {
        let load_version
            = self.load_version();

        read_latch == load_version & !PIN_FLAG_VERSION && load_version & WRITE_OBSOLETE_FLAG_VERSION == 0
    }

    #[inline(always)]
    pub fn pin_write_lock(&self, read_version_pin: LatchVersion) -> LatchVersion {
        let pin_write
            = WRITE_FLAG_VERSION | (read_version_pin & !PIN_FLAG_VERSION);

        self.cell_version.store(pin_write, Relaxed);

        pin_write
    }

    #[cfg(not(all(feature = "olc-hle", any(target_arch = "x86", target_arch = "x86_64"))))]
    #[inline(always)]
    pub fn write_lock(&self, read_version: LatchVersion) -> Option<LatchVersion> {
        match self.cell_version.compare_exchange_weak(
            read_version,
            WRITE_FLAG_VERSION | read_version,
            AcqRel,
            Relaxed)
        {
            Ok(..) => Some(WRITE_FLAG_VERSION | read_version),
            Err(..) => None
        }
    }

    #[cfg(all(feature = "olc-hle", any(target_arch = "x86", target_arch = "x86_64")))]
    #[inline(always)]
    pub fn write_lock(&self, read_version: LatchVersion) -> Option<LatchVersion> {
        match self.cell_version.elision_compare_exchange_acquire(
            read_version,
            WRITE_FLAG_VERSION | read_version)
        {
            Ok(..) => Some(WRITE_FLAG_VERSION | read_version),
            Err(..) => None
        }
    }

    #[inline(always)]
    pub fn write_lock_strong(&self, read_version: LatchVersion) -> Option<LatchVersion> {
        match self.cell_version.compare_exchange(
            read_version,
            WRITE_FLAG_VERSION | read_version,
            AcqRel,
            Relaxed)
        {
            Ok(..) => Some(WRITE_FLAG_VERSION | read_version),
            Err(..) => None
        }
    }

    #[inline(always)]
    pub fn write_unlock(&self, write_version: LatchVersion) {
        debug_assert!(write_version & WRITE_PIN_FLAG_VERSION == WRITE_FLAG_VERSION);

        self.cell_version.store((write_version + 1) ^ WRITE_FLAG_VERSION, Relaxed)
    }

    #[inline(always)]
    pub fn write_obsolete(&self) {
        // debug_assert!(write_version & WRITE_OBSOLETE_FLAG_VERSION == WRITE_FLAG_VERSION);

        self.cell_version.store(OBSOLETE_FLAG_VERSION, Relaxed)
    }

    #[inline(always)]
    pub fn write_obsolete_with_latch(&self, latch: LatchVersion) {
        // debug_assert!(write_version & WRITE_OBSOLETE_FLAG_VERSION == WRITE_FLAG_VERSION);

        self.cell_version.store(OBSOLETE_FLAG_VERSION | latch, Release)
    }

    #[inline(always)]
    pub fn is_obsolete(&self) -> bool {
        self.load_version() == OBSOLETE_FLAG_VERSION
    }

    #[inline(always)]
    pub fn is_write(&self) -> bool {
        self.load_version() & WRITE_FLAG_VERSION == WRITE_FLAG_VERSION
    }

    #[inline(always)]
    pub fn is_read_not_obsolete(&self) -> bool {
        self.load_version() & WRITE_OBSOLETE_FLAG_VERSION == 0
    }

    #[inline(always)]
    pub fn is_read_not_obsolete_result(&self) -> (bool, LatchVersion) {
        if ENABLE_YIELD {
            sched_yield(FORCE_YIELD);
        }

        let read_version
            = self.load_version();

        (read_version & WRITE_OBSOLETE_FLAG_VERSION == 0, read_version)
    }
}

#[derive(Default)]
pub struct SmartCell<E: Default>(pub Arc<SmartFlavor<E>>);

impl<E: Default> Clone for SmartCell<E> {
    #[inline(always)]
    fn clone(&self) -> Self {
        SmartCell(self.0.clone())
    }
}

pub enum SmartFlavor<E: Default> {
    FreeCell(SafeCell<E>),
    ExclusiveCell(Mutex<()>, SafeCell<E>),
    ReadersWriterCell(RwLock<()>, SafeCell<E>),
    OLCCell(OptCell<E>),
    LightWeightHybridCell(OptCell<E>),
    HybridCell(OptCell<E>, RwLock<()>),
}

impl<E: Default> Default for SmartFlavor<E> {
    fn default() -> Self {
        FreeCell(SafeCell::new(E::default()))
    }
}

impl<E: Default> SmartFlavor<E> {
    #[inline(always)]
    fn is_read_valid(&self, read_version: LatchVersion) -> bool {
        match self {
            OLCCell(opt) | LightWeightHybridCell(opt) =>
                opt.is_read_valid(read_version),
            HybridCell(opt, rw) => {
                let reader
                    = rw.try_read();

                reader.is_some() && opt.is_read_valid(read_version)
            }
            _ => true
        }
    }

    #[inline(always)]
    fn is_read_not_obsolete(&self) -> bool {
        match self {
            OLCCell(opt) | LightWeightHybridCell(opt) =>
                opt.is_read_not_obsolete(),
            HybridCell(opt, rw) => {
                let reader
                    = rw.try_read();

                reader.is_some() && opt.is_read_not_obsolete()
            }
            _ => true
        }
    }

    #[inline(always)]
    fn is_read_not_obsolete_result(&self) -> (bool, LatchVersion) {
        match self {
            OLCCell(opt) | LightWeightHybridCell(opt) =>
                opt.is_read_not_obsolete_result(),
            HybridCell(opt, rw) => {
                let reader
                    = rw.try_read();

                if reader.is_some() {
                    let (read, latch)
                        = opt.is_read_not_obsolete_result();

                    (read, latch)
                } else {
                    (false, LatchVersion::MIN)
                }
            }
            _ => (true, LatchVersion::MIN)
        }
    }
}

impl<E: Default + 'static> Deref for SmartFlavor<E> {
    type Target = E;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        match self {
            ExclusiveCell(.., ptr) => ptr.get_mut(),
            OLCCell(opt) | LightWeightHybridCell(opt) =>
                opt.cell.as_ref(),
            HybridCell(opt, _) => opt.cell.as_ref(),
            FreeCell(ptr) => ptr.get_mut(),
            ReadersWriterCell(.., ptr) => ptr.get_mut(),
        }
    }
}

impl<E: Default + 'static> DerefMut for SmartFlavor<E> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            ExclusiveCell(.., ptr) => ptr.get_mut(),
            OLCCell(opt) | LightWeightHybridCell(opt) =>
                opt.cell.get_mut(),
            HybridCell(opt, _) => opt.cell.get_mut(),
            FreeCell(ptr) => ptr.get_mut(),
            ReadersWriterCell(.., ptr) => ptr.get_mut()
        }
    }
}

pub enum SmartGuard<'a, E: Default> {
    LockFree(*mut E),
    RwReader(RwLockReadGuard<'a, RawRwLock, ()>, *const E),
    RwWriter(RwLockWriteGuard<'a, RawRwLock, ()>, *mut E),
    MutExclusive(MutexGuard<'a, RawMutex, ()>, *mut E),
    OLCReader(Option<(SmartCell<E>, LatchVersion)>),
    OLCWriter(SmartCell<E>, LatchVersion),
    OLCReaderPin(SmartCell<E>, LatchVersion),
    HybridRwReader(RwLockReadGuard<'a, RawRwLock, ()>, &'a OptCell<E>, LatchVersion),
    HybridRwWriter(RwLockWriteGuard<'a, RawRwLock, ()>, &'a OptCell<E>, LatchVersion),
}

impl<'a, E: Default + 'static> Clone for SmartGuard<'_, E> {
    fn clone(&self) -> Self {
        match self {
            OLCReader(inner) => OLCReader(inner.clone()),
            OLCReaderPin(inner, read_latch) =>
                OLCReader(Some((inner.clone(), (*read_latch & !PIN_FLAG_VERSION)))),
            RwReader(guard, ptr) => RwReader(
                RwLockReadGuard::rwlock(guard)
                    .read(),
                *ptr),
            LockFree(ptr) => LockFree(*ptr),
            _ => OLCReader(None)
        }
    }
}

impl<'a, E: Default + 'static> SmartGuard<'_, E> {
    #[inline(always)]
    pub(crate) fn mark_obsolete(&mut self) {
        match self {
            OLCWriter(cell, latch) => match cell.0.as_ref() {
                OLCCell(opt) | HybridCell(opt, ..) =>
                    opt.write_obsolete(),
                LightWeightHybridCell(opt) => {
                    opt.write_obsolete();
                    *latch = ZEROED_FLAG_VERSION
                }
                _ => {}
            }
            HybridRwWriter(_, opt, latch) => {
                opt.write_obsolete_with_latch(*latch);
                *latch |= OBSOLETE_FLAG_VERSION;
            }
            _ => {}
        }
    }

    #[inline(always)]
    pub fn downgrade(&mut self) {
        match self {
            RwWriter(guard, ptr) => unsafe {
                let reader
                    = RwLockWriteGuard::downgrade(mem::transmute_copy(guard));

                let s_guard
                    = RwReader(reader, *ptr);

                ptr::write(self, s_guard)
            },
            // OLCWriter(cell, latch)
            // if *latch & OBSOLETE_FLAG_VERSION == 0 => {
            //     let reader
            //         = OLCReader(Some((cell.clone(), *latch & !WRITE_FLAG_VERSION)));
            //
            //     let _ = mem::replace(self, reader);
            // }
            _ => {}
        }
    }

    #[inline(always)]
    pub fn upgrade_write_lock(&mut self) -> bool {
        match self {
            LockFree(_) => true,
            RwWriter(..) => true,
            HybridRwWriter(..) => true,
            MutExclusive(..) => true,
            OLCWriter(..) => true,
            RwReader(reader, ptr) => unsafe {
                let rw = RwLockReadGuard::rwlock(reader);
                rw.force_unlock_read();

                if let Some(writer) = rw.try_write() {
                    let ptr = (*ptr) as *mut _;
                    ptr::write(self, RwWriter(writer, ptr));
                    return true;
                }

                ptr::write(self, OLCReader(None));
                false
            }
            OLCReader(Some((ref cell, read_latch))) => unsafe {
                match cell.0.as_ref() {
                    OLCCell(opt) | LightWeightHybridCell(opt) => if let Some(write_latch)
                        = opt.write_lock(*read_latch)
                    {
                        let writer = OLCWriter(transmute_copy(cell), write_latch);
                        ptr::write(self, writer);
                        return true;
                    },
                    // LightWeightHybridCell(opt) => if let Some(write_latch)
                    //     = opt.write_lock(*read_latch)
                    // {
                    //     let writer = OLCWriter(transmute_copy(cell), write_latch);
                    //     ptr::write(self, writer);
                    //     return true;
                    // } else {
                    //     let read_latch
                    //         = opt.load_version();
                    //
                    //     if read_latch & WRITE_PIN_OBSOLETE_FLAG_VERSION != 0 {
                    //         return false;
                    //     }
                    //     if let Some(write_latch) = opt.write_lock(read_latch) {
                    //         let writer = OLCWriter(transmute_copy(cell), write_latch);
                    //         ptr::write(self, writer);
                    //         // std::sync::atomic::fence(AcqRel);
                    //         return true;
                    //     }
                    // }
                    HybridCell(opt, rw) => if let Some(guard)
                        = rw.try_write()
                    {
                        let read_latch
                            = opt.load_version();

                        if read_latch & WRITE_OBSOLETE_FLAG_VERSION != 0 {
                            return false;
                        }

                        if let Some(write_latch) = opt.write_lock_strong(read_latch) {
                            // mem::drop(guard);
                            // let writer = OLCWriter(transmute_copy(cell), write_latch);
                            // ptr::write(self as *const _ as *mut Self, writer);
                            let writer = transmute(HybridRwWriter(
                                guard,
                                opt,
                                write_latch));

                            ptr::write(self as *const _ as *mut Self, writer);
                            return true;
                        }
                    }
                    _ => {}
                }

                false
            }
            OLCReaderPin(cell, pin_latch) => unsafe {
                if let LightWeightHybridCell(opt) = cell.0.as_ref() {
                    let writer = OLCWriter(
                        transmute_copy(cell),
                        opt.pin_write_lock(*pin_latch));

                    ptr::write(self, writer);
                    return true;
                }

                unreachable!()
            }
            _ => false
        }
    }

    #[inline(always)]
    pub const fn is_write_lock(&self) -> bool {
        match self {
            RwWriter(..) => true,
            MutExclusive(..) => true,
            OLCWriter(..) => true,
            HybridRwWriter(..) => true,
            _ => false
        }
    }

    #[inline(always)]
    pub const fn is_reader_lock(&self) -> bool {
        !self.is_write_lock()
    }

    #[inline(always)]
    pub const fn is_olc_lock(&self) -> bool {
        match self {
            OLCReader(..) => true,
            OLCWriter(..) => true,
            _ => false
        }
    }

    #[inline(always)]
    pub fn is_valid(&self) -> bool {
        match self {
            OLCReader(Some((cell, latch))) => cell.0
                .is_read_valid(*latch),
            OLCReader(None) => false,
            HybridRwReader(.., opt, latch) =>
                opt.is_read_valid(*latch),
            //  | HybridRwWriter(.., opt, latch)
            _ => true
        }
    }

    #[inline(always)]
    pub fn is_read_not_obsolete(&self) -> bool {
        match self {
            OLCReader(Some((cell, ..))) => cell.0.is_read_not_obsolete(),
            OLCReader(None) => false,
            HybridRwReader(.., opt, _) |
            HybridRwWriter(.., opt, _) => opt.is_read_not_obsolete(),
            _ => true
        }
    }

    #[inline(always)]
    pub unsafe fn update_read_latch(&mut self, read_latch: LatchVersion) {
        if let OLCReader(Some((.., latched))) = self {
            *latched = read_latch
        }
    }

    #[inline(always)]
    pub fn is_read_not_obsolete_result(&self) -> (IsRead, LatchVersion) {
        match self {
            OLCReader(Some((cell, ..))) => cell.0.is_read_not_obsolete_result(),
            OLCReader(None) => (false, LatchVersion::MIN),
            OLCReaderPin(.., latch) => (true, *latch & !PIN_FLAG_VERSION),
            HybridRwReader(.., opt, _) |
            HybridRwWriter(.., opt, _) =>
                opt.is_read_not_obsolete_result(),
            _ => (true, LatchVersion::MIN)
        }
    }

    #[inline(always)]
    pub fn deref(&self) -> Option<&'_ E> {
        match self {
            LockFree(ptr) => unsafe { ptr.as_ref() },
            RwReader(.., ptr) => unsafe { ptr.as_ref() },
            RwWriter(.., ptr) => unsafe { ptr.as_ref() },
            MutExclusive(.., ptr) => unsafe { ptr.as_ref() },
            OLCReader(Some((cell, latch))) if cell.0.is_read_valid(*latch) =>
                Some(cell.0.as_ref()),
            OLCWriter(cell, ..) => Some(cell.0.as_ref()),
            OLCReaderPin(cell, ..) => Some(cell.0.as_ref()),
            HybridRwReader(.., opt, _) | HybridRwWriter(_, opt, ..) =>
                Some(opt.cell.as_ref()),
            _ => None
        }
    }

    #[inline(always)]
    pub unsafe fn deref_unsafe(&self) -> Option<&'_ E> {
        match self {
            LockFree(ptr) => ptr.as_ref(),
            RwReader(.., ptr) => ptr.as_ref(),
            RwWriter(.., ptr) => ptr.as_ref(),
            MutExclusive(.., ptr) => ptr.as_ref(),
            OLCReader(Some((cell, ..))) => Some(cell.0.as_ref()),
            OLCWriter(cell, ..) => Some(cell.0.as_ref()),
            OLCReaderPin(cell, ..) =>
                Some(cell.0.as_ref()),
            HybridRwReader(.., opt, _) | HybridRwWriter(_, opt, ..) =>
                Some(opt.cell.as_ref()),
            _ => None
        }
    }

    #[inline(always)]
    pub fn deref_mut(&self) -> Option<&mut E> {
        match self {
            LockFree(ptr) => unsafe { ptr.as_mut() },
            RwWriter(.., ptr) => unsafe { ptr.as_mut() },
            MutExclusive(.., ptr) => unsafe { ptr.as_mut() },
            OLCWriter(cell, ..) => Some(cell.unsafe_borrow_mut()),
            // OLCReaderPin(cell, ..) =>
            //     if let LightWeightHybridCell(opt) = cell.0.as_ref() {
            //         Some(opt.cell.get_mut())
            //     } else {
            //         unreachable!()
            //     }
            HybridRwWriter(_, opt, ..) => Some(opt.cell.get_mut()),
            _ => None
        }
    }
}

impl<E: Default> SmartCell<E> {
    #[inline(always)]
    pub fn unsafe_borrow(&self) -> &E {
        match self.0.as_ref() {
            ExclusiveCell(.., ptr) => ptr.as_ref(),
            OLCCell(opt) | LightWeightHybridCell(opt) =>
                opt.cell.as_ref(),
            HybridCell(opt, _) => opt.cell.as_ref(),
            FreeCell(ptr) => ptr.as_ref(),
            ReadersWriterCell(.., ptr) => ptr.as_ref(),
        }
    }

    #[inline(always)]
    pub fn unsafe_borrow_mut(&self) -> &mut E {
        match self.0.as_ref() {
            ExclusiveCell(.., ptr) => ptr.get_mut(),
            OLCCell(opt) | LightWeightHybridCell(opt) =>
                opt.cell.get_mut(),
            HybridCell(opt, ..) => opt.cell.get_mut(),
            FreeCell(ptr) => ptr.get_mut(),
            ReadersWriterCell(.., ptr) => ptr.get_mut()
        }
    }

    #[inline(always)]
    pub fn borrow_free(&self) -> SmartGuard<'static, E> {
        match self.0.deref() {
            FreeCell(ptr) => LockFree(ptr.get_mut()),
            _ => unreachable!()
        }
    }

    #[inline(always)]
    pub fn borrow_read_hybrid(&self) -> SmartGuard<'static, E> {
        match self.0.deref() {
            HybridCell(opt, rw) => unsafe {
                transmute(HybridRwReader(
                    rw.read(),
                    opt,
                    opt.load_version()))
            }
            _ => unreachable!()
        }
    }

    #[inline(always)]
    pub fn borrow_read(&self) -> SmartGuard<'static, E> {
        match self.0.deref() {
            OLCCell(opt) |
            HybridCell(opt, ..) |
            LightWeightHybridCell(opt) => {
                let (success, read)
                    = opt.read_lock();

                OLCReader(success.then(|| (self.clone(), read)))
            }
            ExclusiveCell(mutex, ptr) => unsafe {
                MutExclusive(transmute(mutex.lock()),
                             ptr.get_mut())
            },
            ReadersWriterCell(rw, ptr) => unsafe {
                RwReader(transmute(rw.read()), ptr.as_ref())
            },
            FreeCell(ptr) => LockFree(ptr.get_mut()),
        }
    }

    #[inline(always)]
    pub fn borrow_pin(&self) -> SmartGuard<'static, E> {
        match self.0.deref() {
            LightWeightHybridCell(opt) => match opt.pin_lock() {
                Ok(pin_latch) =>
                    OLCReaderPin(self.clone(), pin_latch),
                Err((true, read_latch)) =>
                    OLCReader(Some((self.clone(), read_latch))),
                _ => OLCReader(None)
            },
            _ => OLCReader(None)
        }
    }

    #[inline(always)]
    pub fn borrow_mut(&self) -> SmartGuard<'static, E> {
        match self.0.deref() {
            FreeCell(ptr) => LockFree(ptr.get_mut()),
            ReadersWriterCell(rw, ptr) => unsafe {
                transmute(RwWriter(
                    transmute(rw.write()),
                    ptr.get_mut(),
                ))
            },
            OLCCell(opt) | LightWeightHybridCell(opt) => {
                let read_version
                    = opt.load_version();

                if read_version & WRITE_PIN_OBSOLETE_FLAG_VERSION != 0 {
                    OLCReader(None)
                } else if let Some(latched) = opt.write_lock(read_version) {
                    OLCWriter(self.clone(), latched)
                } else {
                    OLCReader(None)
                }
            }
            HybridCell(opt, rw) => match rw.try_write() {
                None => OLCReader(None),
                Some(writer) => unsafe {
                    let (read, read_version)
                        = opt.is_read_not_obsolete_result();

                    if !read {
                        OLCReader(None)
                    } else if let Some(write_latch) = opt.write_lock_strong(read_version) {
                        transmute(HybridRwWriter(writer,
                                                 opt,
                                                 write_latch))
                    } else {
                        OLCReader(None)
                    }
                }
            }
            ExclusiveCell(mutex, ptr) => unsafe {
                transmute(MutExclusive(
                    transmute(mutex.lock()),
                    ptr.get_mut(),
                ))
            }
        }
    }
}

impl<'a, E: Default> Drop for SmartGuard<'a, E> {
    fn drop(&mut self) {
        match self {
            #[cfg(feature = "orwc-fair")]
            RwWriter(guard, ..) => unsafe {
                RwLockWriteGuard::rwlock(guard)
                    .force_unlock_write_fair();
                
                ptr::write(self, OLCReader(None))
            }
            #[cfg(feature = "orwc-fair")]
            RwReader(guard, ..) => unsafe {
                RwLockReadGuard::rwlock(guard)
                    .force_unlock_read_fair();
                
                ptr::write(self, OLCReader(None))
            }
            OLCWriter(cell, write_version) =>
                if let LightWeightHybridCell(opt) = cell.0.as_ref() {
                    if *write_version != ZEROED_FLAG_VERSION {
                        opt.write_unlock(*write_version);
                    }
                } else if let OLCCell(opt) | HybridCell(opt, ..) = cell.0.as_ref() {
                    opt.write_unlock(*write_version)
                }
            OLCReaderPin(cell, pin_version) =>
                if let LightWeightHybridCell(opt) = cell.0.as_ref() {
                    opt.write_unpin(*pin_version)
                }
            HybridRwWriter(.., opt, latch) =>
                opt.write_unlock(*latch),
            _ => {}
        }
    }
}

unsafe impl<'a, E: Default + 'a> Sync for SmartGuard<'a, E> {}

unsafe impl<'a, E: Default + 'a> Send for SmartGuard<'a, E> {}