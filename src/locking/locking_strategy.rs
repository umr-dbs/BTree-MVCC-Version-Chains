use std::fmt::{Display, Formatter};
use serde::{Deserialize, Serialize};
use crate::page_model::{Attempts, Height, Level};
use crate::utils::smart_cell::LatchType;

#[inline(always)]
pub const fn OLC() -> LockingStrategy {
    LockingStrategy::OLC
}

#[inline(always)]
pub const fn hybrid_lock() -> LockingStrategy {
    hybrid_lock_attempts(1)
}

#[inline(always)]
pub const fn hybrid_lock_attempts(attempts: Attempts) -> LockingStrategy {
    LockingStrategy::HybridLocking { read_attempt: attempts }
}

#[inline(always)]
pub const fn lightweight_hybrid_lock() -> LockingStrategy {
    LHL_read(4)
}

#[inline(always)]
pub const fn LHL_read(read_attempt: Attempts) -> LockingStrategy {
    LockingStrategy::LightweightHybridLock {
        read_level: 1_f32,
        read_attempt,
        write_level: f32::MAX,
        write_attempt: Attempts::MAX,
    }
}

#[inline(always)]
pub const fn LHL_write(write_attempt: Attempts) -> LockingStrategy {
    LockingStrategy::LightweightHybridLock {
        read_level: f32::MAX,
        read_attempt: Attempts::MAX,
        write_level: 1f32,
        write_attempt,
    }
}

#[inline(always)]
pub const fn LHL_read_write(write_attempt: Attempts, read_attempt: Attempts) -> LockingStrategy {
    LockingStrategy::LightweightHybridLock {
        read_level: 1f32,
        read_attempt,
        write_level: 1f32,
        write_attempt,
    }
}

#[inline(always)]
pub const fn lightweight_hybrid_lock_unlimited() -> LockingStrategy {
    LockingStrategy::LightweightHybridLock {
        read_level: f32::MAX,
        read_attempt: Attempts::MAX,
        write_level: f32::MAX,
        write_attempt: Attempts::MAX,
    }
}

#[inline(always)]
pub const fn orwc() -> LockingStrategy {
    orwc_attempts(4)
}

#[inline(always)]
pub const fn orwc_attempts(attempts: Attempts) -> LockingStrategy {
    LockingStrategy::ORWC { write_level: 1f32, write_attempt: attempts }
}

pub trait LevelExtras {
    fn is_lock(&self, height: Height, lock_from: f32) -> bool;
}

impl LevelExtras for Level {
    #[inline(always)]
    fn is_lock(&self, height: Height, lock_from: f32) -> bool {
        (lock_from * height as f32) as Self <= *self
    }
}

#[derive(Serialize, Deserialize, Default, Clone)]
pub enum LockingStrategy {
    #[default]
    MonoWriter,
    LockCoupling,
    ORWC {
        write_level: f32,
        write_attempt: Attempts,
    },
    OLC,
    LightweightHybridLock {
        read_level: f32,
        read_attempt: Attempts,
        write_level: f32,
        write_attempt: Attempts,
    },
    HybridLocking {
        read_attempt: Attempts,
    },
}

pub type CRUDProtocol = LockingStrategy;

impl Display for LockingStrategy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LockingStrategy::MonoWriter => write!(f, "MonoWriter"),
            LockingStrategy::LockCoupling => write!(f, "LockCoupling"),
            LockingStrategy::ORWC { write_level, write_attempt } =>
                write!(f, "ORWC(Attempts={};Level={}*height)", write_attempt, write_level),
            LockingStrategy::OLC => write!(f, "OLC"),
            LockingStrategy::LightweightHybridLock {
                write_level, read_level, ..
            } if *write_level > 1f32 && *read_level > 1f32 => write!(f, "LHL(Unlimited)"),
            LockingStrategy::LightweightHybridLock {
                write_level, read_level, read_attempt, ..
            } if *write_level > 1f32  => write!(f, "LHL(rAttempts={};rLevel={}*height)",
                                                read_attempt, read_level),
            LockingStrategy::LightweightHybridLock {
                write_level, write_attempt, read_level, ..
            } if *read_level > 1f32  => write!(f, "LHL(wAttempts={};wLevel={}*height)",
                                               write_attempt, write_level),
            LockingStrategy::LightweightHybridLock {
                read_level, read_attempt,
                write_level, write_attempt
            } => write!(f, "LHL(wAttempts={};wLevel={}*height;rAttempts={};rLevel={}*height)",
                        write_attempt, write_level,
                        read_attempt, read_level),
            LockingStrategy::HybridLocking { read_attempt } =>
                write!(f, "HL(Attempts={})", read_attempt),
        }
    }
}

impl LockingStrategy {
    // const KEY_LOCKING_STRATEGY: &'static str = "lockingstrategy";
    //
    // pub(crate) fn load(configs: &HashMap<String, String>) -> LockingStrategy {
    //     match configs.get(Self::KEY_LOCKING_STRATEGY) {
    //         None => LockingStrategy::default(),
    //         Some(inner) => serde_json::from_str(inner)
    //             .ok()
    //             .unwrap_or(LockingStrategy::default())
    //     }
    // }

    #[inline(always)]
    pub const fn latch_type(&self) -> LatchType {
        match self {
            LockingStrategy::MonoWriter => LatchType::None,
            LockingStrategy::LockCoupling => LatchType::Exclusive,
            LockingStrategy::ORWC { .. } => LatchType::ReadersWriter,
            LockingStrategy::OLC =>
                LatchType::Optimistic,
            LockingStrategy::LightweightHybridLock { .. } =>
                LatchType::LightWeightHybrid,
            LockingStrategy::HybridLocking { .. } =>
                LatchType::Hybrid
        }
    }

    #[inline(always)]
    pub(crate) const fn is_optimistic(&self) -> bool {
        match self {
            Self::OLC => true,
            Self::HybridLocking { .. } => true,
            Self::LightweightHybridLock { .. } => true,
            _ => false
        }
    }

    #[inline(always)]
    pub(crate) const fn is_mono_writer(&self) -> bool {
        match self {
            Self::MonoWriter => true,
            _ => false
        }
    }

    #[inline(always)]
    pub(crate) const fn is_orwc(&self) -> bool {
        match self {
            Self::ORWC { .. } => true,
            _ => false
        }
    }

    #[inline(always)]
    pub(crate) const fn is_hybrid_lock(&self) -> bool {
        match self {
            Self::HybridLocking { .. } => true,
            _ => false
        }
    }

    #[inline(always)]
    pub(crate) const fn is_lightweight_hybrid_lock(&self) -> bool {
        match self {
            Self::LightweightHybridLock { .. } => true,
            _ => false
        }
    }

    #[inline(always)]
    pub const fn additional_lock_required(&self) -> bool {
        match self {
            Self::MonoWriter => false,
            Self::LockCoupling => false,
            _ => true,
        }
    }

    // #[inline(always)]
    // pub fn is_lock_root(&self, lock_level: Level, attempt: Attempts, height: Level) -> bool {
    //     self.is_lock(
    //         LEVEL_ROOT,
    //         lock_level,
    //         attempt,
    //         height)
    // }
    //
    // #[inline(always)]
    // fn is_lock(
    //     &self,
    //     curr_level: Level,
    //     max_level: Level,
    //     attempt: Attempts,
    //     height: Level,
    // ) -> bool {
    //     match self {
    //         Self::LockCoupling => true,
    //         Self::ORWC { write_level, write_attempt } =>
    //             curr_level >= height
    //                 || curr_level >= max_level
    //                 || attempt >= *write_attempt
    //                 || curr_level.is_lock(height, *write_level),
    //         Self::LightweightHybridLock { write_level, write_attempt, .. } if *write_level <= 1_f32 =>
    //             curr_level >= height
    //                 || curr_level >= max_level
    //                 || attempt >= *write_attempt
    //                 || curr_level.is_lock(height, *write_level),
    //         _ => false
    //     }
    // }
}