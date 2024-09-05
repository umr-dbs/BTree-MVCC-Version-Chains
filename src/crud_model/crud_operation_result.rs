use std::fmt::{Display, Formatter};
use std::hash::Hash;
use crate::record_model::record_point::RecordPoint;
use crate::crud_model::crud_operation_result::CRUDOperationResult::{Deleted, Inserted, MatchedRecord, MatchedRecords, Updated};

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
    Inserted(Key),
    Updated(Key, Payload),
    Deleted(Key, Payload),

    #[default]
    Error, // flatten no good
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
            Inserted(key) =>
                write!(f, "Inserted(key: {})",
                       key),
            Updated(key, payload) =>
                write!(f, "Updated(key: {}, payload: {})",
                       key,
                       payload),
            Deleted(key, payload) =>
                write!(f, "Deleted(key: {}, version: {})",
                       key,
                       payload),

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