use std::hash::Hash;
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;

pub type NodeVisits = usize;
pub trait CRUDDispatcher<
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone
> {
    fn dispatch(&self,
                operation: CRUDOperation<Key, Payload>
    ) -> (NodeVisits, CRUDOperationResult<Key, Payload>);
}