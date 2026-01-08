use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::sync::atomic::AtomicU64;

/// Declares the version type.
pub type Version = u64;




/// Declares the atomic version type.
pub type AtomicVersion = AtomicU64;

/// Defines a deleted version, wrapping with one leading marker bit.
#[derive(Clone, Default, Copy, Hash)]
pub struct DeletedVersionInfo(Version);

/// Implements mapping for deletion versions.
impl DeletedVersionInfo {
    /// Defines the mask of a non-null Version, i.e. where the left outer most bit is set.
    /// Otherwise, defines a null mapping and thus does not exist.
    const NON_NULL_FLAG: Version = 0x80_00000000000000;

    /// Defines the null instance.
    const NULL_FLAG: Version = 0;

    /// The actual mask for selecting the Version.
    const EXTRACTOR: Version = 0x7F_FFFFFFFFFFFFFF;

    /// Standard initializer.
    #[inline(always)]
    pub const fn new_null() -> Self {
        Self(Self::NULL_FLAG)
    }

    /// Initializer with a deletion version.
    #[inline(always)]
    const fn new(del_version: Version) -> Self {
        Self(del_version | Self::NON_NULL_FLAG)
    }

    /// Retrieves the underlying Delete-Version if present, otherwise None is returned.
    #[inline(always)]
    pub const fn get(&self) -> Option<Version> {
        match self.0 & Self::NON_NULL_FLAG {
            0 => None,
            _ => Some(self.0 & Self::EXTRACTOR),
        }
    }
}

/// Sugar implementation, wrapping a deletion version.
impl Into<DeletedVersionInfo> for Version {
    fn into(self) -> DeletedVersionInfo {
        DeletedVersionInfo::new(self)
    }
}

/// Defines the version information structure, i.e. insert and delete version.
#[derive(Clone, Default, Copy, Hash)]
pub struct VersionInfo {
    pub insert_version: Version,
    delete_version: DeletedVersionInfo,
}

impl PartialEq for VersionInfo {
    fn eq(&self, other: &Self) -> bool {
        self.insert_version == other.insert_version
    }
}

impl PartialOrd for VersionInfo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.insert_version.partial_cmp(&other.insert_version)
    }
}

impl Eq for VersionInfo {}

impl Ord for VersionInfo {
    fn cmp(&self, other: &Self) -> Ordering {
        self.insert_version.cmp(&other.insert_version)
    }
}

/// Sugar implementation, wrapping a version into a VersionInfo.
impl Into<VersionInfo> for Version {
    #[inline(always)]
    fn into(self) -> VersionInfo {
        VersionInfo::new(self)
    }
}

/// Managing methods implementation for VersionInfo.
impl VersionInfo {
    /// Basic constructor, setting insertion version via supplied version and deletion to None.
    #[inline(always)]
    pub const fn new(insert_version: Version) -> Self {
        Self {
            insert_version,
            delete_version: DeletedVersionInfo::new_null(),
        }
    }

    /// Extended constructor, setting both fields via supplied parameters.
    #[inline(always)]
    pub fn from(insert_version: Version, delete_version: Version) -> Self {
        Self {
            insert_version,
            delete_version: delete_version.into(),
        }
    }

    /// Returns true, if supplied version matches.
    /// Returns false, otherwise.
    #[inline(always)]
    pub fn matches(&self, version: Version) -> bool {
        self.insert_version <= version
            && self
                .delete_version
                .get()
                .map(|del| del > version)
                .unwrap_or(true)
    }

    /// Retrieves the insertion version.
    #[inline(always)]
    pub const fn insertion_version(&self) -> Version {
        self.insert_version
    }

    /// Retrieves the deletion version.
    #[inline(always)]
    pub const fn deletion_version(&self) -> Option<Version> {
        self.delete_version.get()
    }

    /// Returns true, if this version has been deleted.
    #[inline(always)]
    pub const fn is_deleted(&self) -> bool {
        self.delete_version.get().is_some()
    }

    /// Actively deletes this version by setting deletion to supplied delete version.
    #[inline(always)]
    pub fn delete(&mut self, delete_version: Version) -> bool {
        debug_assert!(!self.is_deleted());

        if self.is_deleted() {
            false
        } else {
            self.delete_version = delete_version.into();
            true
        }
    }
}

/// Implements standard pretty printers for VersionInfo, displaying both insertion and deletion versions.
impl Display for VersionInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(insert: {}, deleted: {})",
            self.insert_version,
            self.delete_version
                .get()
                .map(|del| del.to_string())
                .unwrap_or("*".to_string())
        )
    }
}
