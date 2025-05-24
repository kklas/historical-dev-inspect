use std::sync::Arc;
use sui_core::authority::authority_store_types::{
    StoreData, StoreObject, StoreObjectV1, StoreObjectValue, StoreObjectWrapper,
};
use sui_types::base_types::{FullObjectID, MoveObjectType, ObjectRef, VersionNumber};
use sui_types::digests::{ObjectDigest, TransactionDigest};
use sui_types::error::{SuiError, UserInputError};
use sui_types::object::{Data, MoveObject, ObjectInner, Owner};
use sui_types::storage::{FullObjectKey, ObjectKey, ObjectOrTombstone, PackageObject};
use sui_types::sui_system_state::{get_sui_system_state, SuiSystemState};
use sui_types::{
    base_types::{ObjectID, SequenceNumber},
    committee::EpochId,
    error::SuiResult,
    object::Object,
    storage::{BackingPackageStore, ChildObjectResolver, ObjectStore, ParentSync},
};
use thiserror::Error;

use crate::db::{HistoricalDb, HistoricalDbError};

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("Invalid historical point: {0}")]
    InvalidHistoricalPoint(String),
    #[error("Database error: {0}")]
    DbError(#[from] HistoricalDbError),
}

pub enum HistoricalPoint {
    BeforeCheckpoint(u64),
    AfterCheckpoint(u64),
    BeforeTransaction {
        checkpoint_seq_num: u64,
        tx_seq_num: u64,
    },
    AfterTransaction {
        checkpoint_seq_num: u64,
        tx_seq_num: u64,
    },
}

impl HistoricalPoint {
    pub fn new_for_checkpoint(checkpoint_seq_num: u64, before: bool) -> Self {
        if before {
            Self::BeforeCheckpoint(checkpoint_seq_num)
        } else {
            Self::AfterCheckpoint(checkpoint_seq_num)
        }
    }

    pub fn new_for_transaction(
        db: &HistoricalDb,
        tx_digest: TransactionDigest,
        before: bool,
    ) -> Result<Self, HistoricalDbError> {
        let (checkpoint_seq_num, tx_seq_num) = db
            .get_tx_chk_seq_num(tx_digest)?
            .ok_or(HistoricalDbError::NotFound)?;
        Ok(if before {
            Self::BeforeTransaction {
                checkpoint_seq_num,
                tx_seq_num,
            }
        } else {
            Self::AfterTransaction {
                checkpoint_seq_num,
                tx_seq_num,
            }
        })
    }
}

pub struct HistoricalView {
    db: Arc<HistoricalDb>,
    point: HistoricalPoint,
}

impl HistoricalView {
    pub fn new(db: Arc<HistoricalDb>, point: HistoricalPoint) -> Result<Self, StoreError> {
        let watermark = db.get_checkpoint_watermark()?;

        if watermark.is_none() {
            return Err(StoreError::InvalidHistoricalPoint(
                "DB is not initialized".to_string(),
            ));
        }
        let watermark = watermark.unwrap();

        let checkpoint_seq_num = match &point {
            HistoricalPoint::BeforeCheckpoint(seq) => *seq,
            HistoricalPoint::AfterCheckpoint(seq) => *seq,
            HistoricalPoint::BeforeTransaction {
                checkpoint_seq_num, ..
            } => *checkpoint_seq_num,
            HistoricalPoint::AfterTransaction {
                checkpoint_seq_num, ..
            } => *checkpoint_seq_num,
        };

        if watermark < checkpoint_seq_num {
            return Err(StoreError::InvalidHistoricalPoint(format!(
                "DB is not initialized to the specified checkpoint: {} < {}",
                watermark, checkpoint_seq_num
            )));
        }

        Ok(Self { db, point })
    }

    /// Returns the checkpoint and transaction sequence numbers to use for
    /// "less than or equal to" (lte) queries in the database, based on the
    /// current historical point. This determines the latest state visible
    /// at the specified point in history. Returns `None` if there is no
    /// valid state at or before the point (e.g., before the first checkpoint).
    fn get_lte_tx_args(&self) -> Option<(u64, u64)> {
        match self.point {
            HistoricalPoint::BeforeCheckpoint(checkpoint_seq_num) => {
                if checkpoint_seq_num == 0 {
                    return None;
                }
                Some((checkpoint_seq_num - 1, u64::MAX))
            }
            HistoricalPoint::AfterCheckpoint(checkpoint_seq_num) => {
                Some((checkpoint_seq_num, u64::MAX))
            }
            HistoricalPoint::BeforeTransaction {
                checkpoint_seq_num,
                tx_seq_num,
            } => {
                if checkpoint_seq_num == 0 && tx_seq_num == 0 {
                    None
                } else if tx_seq_num == 0 {
                    Some((checkpoint_seq_num - 1, u64::MAX))
                } else {
                    Some((checkpoint_seq_num, tx_seq_num - 1))
                }
            }
            HistoricalPoint::AfterTransaction {
                checkpoint_seq_num,
                tx_seq_num,
            } => Some((checkpoint_seq_num, tx_seq_num)),
        }
    }

    fn get_object_data(
        &self,
        object_id: &ObjectID,
    ) -> Result<Option<(ObjectKey, StoreObjectWrapper)>, HistoricalDbError> {
        self.get_lte_tx_args()
            .map(|(checkpoint_seq_num, tx_seq_num)| {
                self.db
                    .get_highest_object_data_lte_tx(*object_id, checkpoint_seq_num, tx_seq_num)
            })
            .unwrap_or(Ok(None))
    }

    pub fn get_object(&self, object_id: &ObjectID) -> Option<Object> {
        let obj_res = self.get_object_data(object_id);

        if let Some((object_key, StoreObjectWrapper::V1(StoreObjectV1::Value(obj_value)))) =
            obj_res.ok().flatten()
        {
            Some(try_construct_object(&object_key, obj_value).expect("object construction error"))
        } else {
            None
        }
    }

    pub fn get_package_object(&self, package_id: &ObjectID) -> SuiResult<Option<PackageObject>> {
        let obj = self.get_object(package_id);
        if let Some(obj) = obj {
            if obj.is_package() {
                Ok(Some(PackageObject::new(obj)))
            } else {
                Err(SuiError::UserInputError {
                    error: UserInputError::MoveObjectAsPackage {
                        object_id: *package_id,
                    },
                })
            }
        } else {
            Ok(None)
        }
    }

    fn get_latest_object_or_tombstone(
        &self,
        object_id: ObjectID,
    ) -> Option<(ObjectKey, ObjectOrTombstone)> {
        let (obj_key, obj_wrapper) = self.get_object_data(&object_id).expect("db error")?;

        match obj_wrapper.clone().into_inner() {
            StoreObject::Value(obj_value) => Some((
                obj_key,
                ObjectOrTombstone::Object(
                    try_construct_object(&obj_key, obj_value.clone())
                        .expect("object construction error"),
                ),
            )),
            StoreObject::Deleted => Some((
                obj_key,
                ObjectOrTombstone::Tombstone((
                    object_id,
                    obj_key.1,
                    ObjectDigest::OBJECT_DIGEST_DELETED,
                )),
            )),
            StoreObject::Wrapped => Some((
                obj_key,
                ObjectOrTombstone::Tombstone((
                    object_id,
                    obj_key.1,
                    ObjectDigest::OBJECT_DIGEST_WRAPPED,
                )),
            )),
        }
    }

    fn get_latest_object_ref_or_tombstone(&self, object_id: ObjectID) -> Option<ObjectRef> {
        match self.get_latest_object_or_tombstone(object_id) {
            Some((_, ObjectOrTombstone::Object(obj))) => Some(obj.compute_object_reference()),
            Some((_, ObjectOrTombstone::Tombstone(obj_ref))) => Some(obj_ref),
            None => None,
        }
    }

    /// Returns `Some(max_version)` if the given object version is less than or equal to the latest
    /// version available at the current historical point (as determined by checkpoint and transaction
    /// sequence numbers). Returns `None` if the version is greater than the latest known version at
    /// this point, or if the object does not exist at or before this point. This does not guarantee
    /// that the exact version exists, only that it is not greater than the latest known version.
    fn object_version_lte_latest_at_point(
        &self,
        object_id: ObjectID,
        version: VersionNumber,
    ) -> Option<VersionNumber> {
        let (checkpoint_seq_num, tx_seq_num) = self.get_lte_tx_args()?;
        if let Some(max_version_at_point) = self
            .db
            .get_highest_object_version_lte_tx(object_id, checkpoint_seq_num, tx_seq_num)
            .expect("db error")
        {
            if version > max_version_at_point {
                return None;
            }
            Some(max_version_at_point)
        } else {
            None
        }
    }

    fn get_object_by_key(&self, object_id: &ObjectID, version: VersionNumber) -> Option<Object> {
        // Check if the version exists at the current point in time
        self.object_version_lte_latest_at_point(*object_id, version)?;

        self.db
            .get_object(*object_id, version)
            .expect("db error")
            .and_then(|wrapper| match wrapper {
                StoreObjectWrapper::V1(StoreObjectV1::Value(obj_value)) => Some(
                    try_construct_object(&ObjectKey(*object_id, version), obj_value)
                        .expect("object construction error"),
                ),
                _ => None,
            })
    }

    fn multi_get_objects_by_key(&self, object_keys: &[ObjectKey]) -> Vec<Option<Object>> {
        object_keys
            .iter()
            .map(|object_key| self.get_object_by_key(&object_key.0, object_key.1))
            .collect()
    }

    fn find_object_lt_or_eq_version(
        &self,
        object_id: ObjectID,
        version: SequenceNumber,
    ) -> Option<Object> {
        // Use the latest version at the current point in time as an upper bound
        let max_version = self.object_version_lte_latest_at_point(object_id, version)?;
        let version = version.min(max_version);

        self.db
            .get_object_lt_or_eq_version(object_id, version)
            .expect("db error")
            .and_then(|wrapper| match wrapper {
                StoreObjectWrapper::V1(StoreObjectV1::Value(obj_value)) => Some(
                    try_construct_object(&ObjectKey(object_id, version), obj_value)
                        .expect("object construction error"),
                ),
                _ => None,
            })
    }

    pub fn have_received_object_at_version(
        &self,
        object_key: FullObjectKey,
        _epoch_id: EpochId,
    ) -> bool {
        // TODO: Handle ConsensusV2 objects
        let object_id = object_key.id().id();
        let version = object_key.version();

        if self
            .object_version_lte_latest_at_point(object_id, version)
            .is_none()
        {
            return false;
        }

        self.db
            .have_received_object_at_version(object_id, version)
            .expect("db error")
    }

    fn get_live_objref(&self, object_id: ObjectID) -> SuiResult<ObjectRef> {
        match self.get_object(&object_id) {
            Some(obj) => Ok(obj.compute_object_reference()),
            None => Err(SuiError::UserInputError {
                error: UserInputError::ObjectNotFound {
                    object_id,
                    version: None,
                },
            }),
        }
    }

    /// Load a list of objects from the store by object reference.
    /// If they exist in the store, they are returned directly.
    /// If any object missing, we try to figure out the best error to return.
    /// If the object we are asking is currently locked at a future version, we know this
    /// transaction is out-of-date and we return a ObjectVersionUnavailableForConsumption,
    /// which indicates this is not retriable.
    /// Otherwise, we return a ObjectNotFound error, which indicates this is retriable.
    pub fn multi_get_objects_with_more_accurate_error_return(
        &self,
        object_refs: &[ObjectRef],
    ) -> Result<Vec<Object>, SuiError> {
        let objects = self
            .multi_get_objects_by_key(&object_refs.iter().map(ObjectKey::from).collect::<Vec<_>>());
        let mut result = Vec::new();
        for (object_opt, object_ref) in objects.into_iter().zip(object_refs) {
            match object_opt {
                None => {
                    let live_objref = self.get_live_objref(object_ref.0)?;
                    let error = if live_objref.1 >= object_ref.1 {
                        UserInputError::ObjectVersionUnavailableForConsumption {
                            provided_obj_ref: *object_ref,
                            current_version: live_objref.1,
                        }
                    } else {
                        UserInputError::ObjectNotFound {
                            object_id: object_ref.0,
                            version: Some(object_ref.1),
                        }
                    };
                    return Err(SuiError::UserInputError { error });
                }
                Some(object) => {
                    result.push(object);
                }
            }
        }
        assert_eq!(result.len(), object_refs.len());
        Ok(result)
    }

    pub fn get_sui_system_state(&self) -> Result<SuiSystemState, SuiError> {
        get_sui_system_state(self)
    }
}

impl ChildObjectResolver for HistoricalView {
    fn read_child_object(
        &self,
        parent: &ObjectID,
        child: &ObjectID,
        child_version_upper_bound: SequenceNumber,
    ) -> SuiResult<Option<Object>> {
        let Some(child_object) =
            self.find_object_lt_or_eq_version(*child, child_version_upper_bound)
        else {
            return Ok(None);
        };

        let parent = *parent;
        if child_object.owner != Owner::ObjectOwner(parent.into()) {
            return Err(SuiError::InvalidChildObjectAccess {
                object: *child,
                given_parent: parent,
                actual_owner: child_object.owner.clone(),
            });
        }
        Ok(Some(child_object))
    }

    fn get_object_received_at_version(
        &self,
        owner: &ObjectID,
        receiving_object_id: &ObjectID,
        receive_object_at_version: SequenceNumber,
        epoch_id: EpochId,
    ) -> SuiResult<Option<Object>> {
        let Some(recv_object) =
            self.get_object_by_key(receiving_object_id, receive_object_at_version)
        else {
            return Ok(None);
        };

        // Check for:
        // * Invalid access -- treat as the object does not exist. Or;
        // * If we've already received the object at the version -- then treat it as though it doesn't exist.
        // These two cases must remain indisguishable to the caller otherwise we risk forks in
        // transaction replay due to possible reordering of transactions during replay.
        if recv_object.owner != Owner::AddressOwner((*owner).into())
            || self.have_received_object_at_version(
                // TODO: Add support for receiving ConsensusV2 objects. For now this assumes fastpath.
                FullObjectKey::new(
                    FullObjectID::new(*receiving_object_id, None),
                    receive_object_at_version,
                ),
                epoch_id,
            )
        {
            return Ok(None);
        }

        Ok(Some(recv_object))
    }
}

impl ObjectStore for HistoricalView {
    fn get_object(&self, object_id: &ObjectID) -> Option<Object> {
        self.get_object(object_id)
    }

    fn get_object_by_key(&self, object_id: &ObjectID, version: VersionNumber) -> Option<Object> {
        self.get_object_by_key(object_id, version)
    }
}

impl BackingPackageStore for HistoricalView {
    fn get_package_object(&self, package_id: &ObjectID) -> SuiResult<Option<PackageObject>> {
        self.get_package_object(package_id)
    }
}

impl ParentSync for HistoricalView {
    fn get_latest_parent_entry_ref_deprecated(&self, object_id: ObjectID) -> Option<ObjectRef> {
        self.get_latest_object_ref_or_tombstone(object_id)
    }
}

pub fn try_construct_object(
    object_key: &ObjectKey,
    store_object: StoreObjectValue,
) -> Result<Object, SuiError> {
    let data = match store_object.data {
        StoreData::Move(object) => Data::Move(object),
        StoreData::Package(package) => Data::Package(package),
        StoreData::Coin(balance) => unsafe {
            Data::Move(MoveObject::new_from_execution_with_limit(
                MoveObjectType::gas_coin(),
                true,
                object_key.1,
                bcs::to_bytes(&(object_key.0, balance)).expect("serialization failed"),
                u64::MAX,
            )?)
        },
        _ => {
            return Err(SuiError::Storage(
                "corrupted field: inconsistent object representation".to_string(),
            ))
        }
    };

    Ok(ObjectInner {
        data,
        owner: store_object.owner,
        previous_transaction: store_object.previous_transaction,
        storage_rebate: store_object.storage_rebate,
    }
    .into())
}
