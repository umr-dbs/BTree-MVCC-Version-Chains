use std::fmt::{Display, Formatter};
use std::hash::Hash;
use crate::record_model::record_point::RecordPoint;
use crate::crud_model::crud_operation_result::CRUDOperationResult::{Deleted, Inserted, MatchedRecord, MatchedRecords, Updated};
use crate::record_model::Version;

/// Defines possible Transaction execution result.
/// *Error*, indicates execution error.
/// *Inserted*, indicates that the Transaction executed was successful and the (key, version) pair
/// of matching record is held.
/// *MatchedRecord*, indicates that the Transaction executed was successful and the result of
/// a potential match is held.
/// *MatchedRecords*, indicates that the Transaction executed was successful and the result of
/// matches is held.
#[derive(Clone, Default)]
pub enum CRUDOperationResult<Key: Ord + Hash + Copy + Default, Payload: Clone + Default> {
    MatchedRecords(Vec<RecordPoint<Key, Payload>>),
    MatchedRecord(Option<RecordPoint<Key, Payload>>),
    Inserted(Key, Version),
    Updated(Key, Payload, Version),
    Deleted(Key, Payload, Version),

    ZeroAffected(CRUDOperationInnerReason),

    #[default]
    Error, // flatten no good
}

#[derive(Clone)]
pub enum CRUDOperationInnerReason {
    KeyAlreadyDeleted,
    KeyDoesNotExist,
}

/// Implements pretty printers for TransactionResult.
impl<Key: Display + Ord + Hash + Copy + Default, Payload: Display + Clone + Default> Display for CRUDOperationResult<Key, Payload> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CRUDOperationResult::Error =>
                write!(f, "Error"),
            MatchedRecord(record) =>
                write!(f, "MatchedRecord({})", record
                    .as_ref()
                    .map(|found| found.to_string())
                    .unwrap_or("None".to_string())),
            MatchedRecords(records) => {
                write!(f, "MatchedRecords[len={}\n", records.len());
                records.iter().for_each(|record| {
                    write!(f, "{}\n", record);
                });
                write!(f, "]")
            }
            Inserted(key, version) =>
                write!(f, "Inserted(key: {}, version: {})",
                       key, version),
            Updated(key, payload, version) =>
                write!(f, "Updated(key: {}, payload: {}, version: {})",
                       key,
                       payload,
                version),
            Deleted(key, payload, version) =>
                write!(f, "Deleted(key: {}, payload: {}, version: {})",
                       key,
                       payload,
                       version),

            CRUDOperationResult::ZeroAffected(CRUDOperationInnerReason::KeyAlreadyDeleted) =>
                write!(f, "ZeroAffected(KeyAlreadyDeleted"),
            CRUDOperationResult::ZeroAffected(CRUDOperationInnerReason::KeyDoesNotExist) =>
                write!(f, "ZeroAffected(KeyDoesNotExist)"),
        }
    }
}

/// Sugar implementation, wrapping collection of records to a TransactionResult.
impl<Key: Ord + Hash + Copy + Default, Payload: Clone + Default> Into<CRUDOperationResult<Key, Payload>> for Vec<RecordPoint<Key, Payload>> {
    fn into(self) -> CRUDOperationResult<Key, Payload> {
        MatchedRecords(self)
    }
}

/// Sugar implementation, wrapping a potential record to a TransactionResult.
impl<Key: Ord + Hash + Copy + Default, Payload: Clone + Default> Into<CRUDOperationResult<Key, Payload>> for Option<RecordPoint<Key, Payload>> {
    fn into(self) -> CRUDOperationResult<Key, Payload> {
        MatchedRecord(self)
    }
}