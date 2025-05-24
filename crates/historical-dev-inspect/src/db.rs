use std::{collections::HashSet, path::Path, sync::Arc};

use rocksdb::{
    BlockBasedOptions, ColumnFamilyDescriptor, DBCompressionType, DBWithThreadMode, IteratorMode,
    MultiThreaded, Options, WriteBatch,
};
use sui_core::authority::authority_store_types::{
    get_store_object, StoreObjectV1, StoreObjectWrapper,
};
use sui_types::{
    base_types::{ObjectID, VersionNumber},
    digests::TransactionDigest,
    effects::TransactionEffectsAPI,
    full_checkpoint_content::CheckpointData,
    object::Object,
    storage::ObjectKey,
    transaction::TransactionDataAPI,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum HistoricalDbError {
    #[error("RocksDB error: {0}")]
    RocksDB(#[from] rocksdb::Error),
    #[error("Failed to open RocksDB at {path}: {error}")]
    Open { path: String, error: rocksdb::Error },
    #[error("Failed to serialize object: {0}")]
    Serialize(#[from] bcs::Error),
    #[error("Invalid value")]
    InvalidValue,
    #[error("Not found")]
    NotFound,
    #[error("Inconsistent database state: {0}")]
    Inconsistent(String),
}

#[derive(Debug, Clone, Copy)]
pub enum HistoricalDbCf {
    /// <tx_digest> -> <checkpoint_seq_num>:<tx_seq_num>
    TxDigestSeqNum,
    /// <object_id>:<version> -> <object_data>
    ObjectData,
    /// <object_id>:<checkpoint_seq_num>:<tx_seq_num> -> <object_version>
    ObjectTxSeqNum,
    /// <object_id>:<version> -> <received_marker>
    ObjectVersionReceived,
    /// <object_id>:<version> -> <object_data>
    BaseObjectSet,
    /// <key> -> <value>
    Meta,
}

impl HistoricalDbCf {
    pub const ALL: [HistoricalDbCf; 6] = [
        HistoricalDbCf::TxDigestSeqNum,
        HistoricalDbCf::ObjectData,
        HistoricalDbCf::ObjectTxSeqNum,
        HistoricalDbCf::ObjectVersionReceived,
        HistoricalDbCf::BaseObjectSet,
        HistoricalDbCf::Meta,
    ];

    pub fn name(self) -> &'static str {
        match self {
            HistoricalDbCf::TxDigestSeqNum => "tx_digest_seq_num",
            HistoricalDbCf::ObjectData => "object_data",
            HistoricalDbCf::ObjectTxSeqNum => "object_tx_seq_num",
            HistoricalDbCf::ObjectVersionReceived => "object_version_received",
            HistoricalDbCf::BaseObjectSet => "base_object_set",
            HistoricalDbCf::Meta => "meta",
        }
    }

    fn options(self) -> Options {
        let mut opts = Options::default();

        if matches!(
            self,
            HistoricalDbCf::ObjectData | HistoricalDbCf::BaseObjectSet
        ) {
            let mut block_opts = BlockBasedOptions::default();
            block_opts.set_block_size(16 * 1024);
            opts.set_block_based_table_factory(&block_opts);
            opts.set_compression_type(DBCompressionType::Zstd);
        }

        opts
    }
}

struct TxChkSeqNumValue {
    checkpoint_seq_num: u64,
    tx_seq_num: u64,
}

impl TxChkSeqNumValue {
    /// Returns a 16-byte array, where the first 8 bytes are the checkpoint_seq_num (u64, BE)
    /// and the last 8 bytes are the tx_seq_num (u64, BE).
    fn as_bytes(&self) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&self.checkpoint_seq_num.to_be_bytes());
        bytes[8..].copy_from_slice(&self.tx_seq_num.to_be_bytes());
        bytes
    }

    /// Parses a 16-byte array into a TxChkSeqNumValue.
    fn from_bytes(bytes: &[u8]) -> Result<Self, HistoricalDbError> {
        if bytes.len() != 16 {
            return Err(HistoricalDbError::InvalidValue);
        }
        Ok(Self {
            checkpoint_seq_num: u64::from_be_bytes(bytes[..8].try_into().unwrap()),
            tx_seq_num: u64::from_be_bytes(bytes[8..].try_into().unwrap()),
        })
    }
}

struct ObjectVersionKey {
    object_id: ObjectID,
    object_version: VersionNumber,
}

impl ObjectVersionKey {
    /// Returns a 40-byte array, where the first 32 bytes are the object_id (32 bytes)
    /// and the last 8 bytes are the object_version (8 bytes, BE).
    fn as_bytes(&self) -> [u8; 40] {
        let mut bytes = [0u8; 40];
        bytes[..32].copy_from_slice(&self.object_id.into_bytes());
        bytes[32..40].copy_from_slice(&self.object_version.value().to_be_bytes());
        bytes
    }

    /// Parses a 40-byte array into an ObjectDataKey.
    fn from_bytes(bytes: &[u8]) -> Result<Self, HistoricalDbError> {
        if bytes.len() != 40 {
            return Err(HistoricalDbError::InvalidValue);
        }
        Ok(Self {
            object_id: ObjectID::from_bytes(&bytes[..32]).unwrap(),
            object_version: VersionNumber::from(u64::from_be_bytes(
                bytes[32..40].try_into().unwrap(),
            )),
        })
    }
}

struct ObjectTxSeqNumKey {
    object_id: ObjectID,
    checkpoint_seq_num: u64,
    tx_seq_num: u64,
}

impl ObjectTxSeqNumKey {
    /// Returns a 48-byte array, where the first 32 bytes are the object_id (32 bytes),
    /// the next 8 bytes are the checkpoint_seq_num (8 bytes, BE), and the last 8 bytes are the tx_seq_num (8 bytes, BE).
    fn as_bytes(&self) -> [u8; 48] {
        let mut bytes = [0u8; 48];
        bytes[..32].copy_from_slice(&self.object_id.into_bytes());
        bytes[32..40].copy_from_slice(&self.checkpoint_seq_num.to_be_bytes());
        bytes[40..48].copy_from_slice(&self.tx_seq_num.to_be_bytes());
        bytes
    }

    /// Parses a 48-byte array into an ObjectTxSeqNumKey.
    fn from_bytes(bytes: &[u8]) -> Result<Self, HistoricalDbError> {
        if bytes.len() != 48 {
            return Err(HistoricalDbError::InvalidValue);
        }
        Ok(Self {
            object_id: ObjectID::from_bytes(&bytes[..32]).unwrap(),
            checkpoint_seq_num: u64::from_be_bytes(bytes[32..40].try_into().unwrap()),
            tx_seq_num: u64::from_be_bytes(bytes[40..48].try_into().unwrap()),
        })
    }
}

struct ObjectTxSeqNumValue(VersionNumber);

impl ObjectTxSeqNumValue {
    fn as_bytes(&self) -> [u8; 8] {
        self.0.value().to_be_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, HistoricalDbError> {
        if bytes.len() != 8 {
            return Err(HistoricalDbError::InvalidValue);
        }
        Ok(Self(VersionNumber::from(u64::from_be_bytes(
            bytes.try_into().unwrap(),
        ))))
    }
}

#[derive(Debug, Clone)]
pub struct HistoricalDb {
    db: Arc<DBWithThreadMode<MultiThreaded>>,
}

impl HistoricalDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, HistoricalDbError> {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let descriptors: Vec<_> = HistoricalDbCf::ALL
            .iter()
            .map(|cf| ColumnFamilyDescriptor::new(cf.name(), cf.options()))
            .collect();

        let db = DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(
            &db_opts,
            path.as_ref(),
            descriptors,
        )
        .map_err(|e| HistoricalDbError::Open {
            path: path.as_ref().to_string_lossy().to_string(),
            error: e,
        })?;

        Ok(HistoricalDb { db: Arc::new(db) })
    }

    pub fn set_base_epoch(&self, epoch: u64) -> Result<(), HistoricalDbError> {
        let cf = self.db.cf_handle(HistoricalDbCf::Meta.name()).unwrap();
        self.db
            .put_cf(&cf, b"base_epoch", &epoch.to_be_bytes())
            .map_err(HistoricalDbError::from)
    }

    pub fn get_base_epoch(&self) -> Result<Option<u64>, HistoricalDbError> {
        let cf = self.db.cf_handle(HistoricalDbCf::Meta.name()).unwrap();
        self.db
            .get_cf(&cf, b"base_epoch")
            .map(|v| v.map(|v| u64::from_be_bytes(v.try_into().unwrap())))
            .map_err(HistoricalDbError::from)
    }

    pub fn set_checkpoint_watermark(
        &self,
        checkpoint_seq_num: u64,
    ) -> Result<(), HistoricalDbError> {
        let cf = self.db.cf_handle(HistoricalDbCf::Meta.name()).unwrap();
        self.db
            .put_cf(
                &cf,
                b"checkpoint_watermark",
                checkpoint_seq_num.to_be_bytes(),
            )
            .map_err(HistoricalDbError::from)
    }

    pub fn get_checkpoint_watermark(&self) -> Result<Option<u64>, HistoricalDbError> {
        let cf = self.db.cf_handle(HistoricalDbCf::Meta.name()).unwrap();
        self.db
            .get_cf(&cf, b"checkpoint_watermark")
            .map(|v| v.map(|v| u64::from_be_bytes(v.try_into().unwrap())))
            .map_err(HistoricalDbError::from)
    }

    fn put_tx_digest_seq_num(
        &self,
        tx_digest: TransactionDigest,
        checkpoint_seq_num: u64,
        tx_seq_num: u64,
    ) -> Result<(), HistoricalDbError> {
        // Store the mapping from tx_digest to (checkpoint_seq_num, tx_seq_num) in the TxDigestSeqNum column family.
        // The key is the 32-byte tx_digest, the value is checkpoint_seq_num (u64, BE) || tx_seq_num (u64, BE).
        let cf = self
            .db
            .cf_handle(HistoricalDbCf::TxDigestSeqNum.name())
            .unwrap();

        let key = tx_digest.inner();
        let value = TxChkSeqNumValue {
            checkpoint_seq_num,
            tx_seq_num,
        }
        .as_bytes();

        self.db
            .put_cf(&cf, key, value)
            .map_err(HistoricalDbError::from)
    }

    fn put_object_data(
        &self,
        object_id: ObjectID,
        object_version: VersionNumber,
        data: StoreObjectWrapper,
    ) -> Result<(), HistoricalDbError> {
        let cf = self
            .db
            .cf_handle(HistoricalDbCf::ObjectData.name())
            .unwrap();

        let key = ObjectVersionKey {
            object_id,
            object_version,
        }
        .as_bytes();
        let value = bcs::to_bytes(&data).map_err(HistoricalDbError::from)?;

        self.db
            .put_cf(&cf, key, value)
            .map_err(HistoricalDbError::from)
    }

    fn put_object_tx_seq_num(
        &self,
        object_id: ObjectID,
        checkpoint_seq_num: u64,
        tx_seq_num: u64,
        object_version: VersionNumber,
    ) -> Result<(), HistoricalDbError> {
        let cf = self
            .db
            .cf_handle(HistoricalDbCf::ObjectTxSeqNum.name())
            .unwrap();

        let key = ObjectTxSeqNumKey {
            object_id,
            checkpoint_seq_num,
            tx_seq_num,
        }
        .as_bytes();
        let value = ObjectTxSeqNumValue(object_version).as_bytes();

        self.db
            .put_cf(&cf, key, value)
            .map_err(HistoricalDbError::from)
    }

    fn put_object_version_received(
        &self,
        object_id: ObjectID,
        object_version: VersionNumber,
    ) -> Result<(), HistoricalDbError> {
        // This column is used just for key-exists lookups, so we only need to insert the key with an empty value.
        let cf = self
            .db
            .cf_handle(HistoricalDbCf::ObjectVersionReceived.name())
            .unwrap();

        let key = ObjectVersionKey {
            object_id,
            object_version,
        }
        .as_bytes();

        self.db
            .put_cf(&cf, key, [])
            .map_err(HistoricalDbError::from)
    }

    pub fn process_checkpoint(&self, data: &CheckpointData) -> Result<(), HistoricalDbError> {
        for (tx_seq_num, tx) in data.transactions.iter().enumerate() {
            self.put_tx_digest_seq_num(
                *tx.transaction.digest(),
                data.checkpoint_summary.sequence_number,
                tx_seq_num as u64,
            )?;

            for obj_ref in tx.effects.deleted().into_iter() {
                let obj_id = obj_ref.0;
                let obj_version = obj_ref.1;
                self.put_object_data(obj_id, obj_version, StoreObjectV1::Deleted.into())?;
                self.put_object_tx_seq_num(
                    obj_id,
                    data.checkpoint_summary.sequence_number,
                    tx_seq_num as u64,
                    obj_version,
                )?;
            }
            for obj_ref in tx.effects.wrapped().into_iter() {
                let obj_id = obj_ref.0;
                let obj_version = obj_ref.1;
                self.put_object_data(obj_id, obj_version, StoreObjectV1::Wrapped.into())?;
                self.put_object_tx_seq_num(
                    obj_id,
                    data.checkpoint_summary.sequence_number,
                    tx_seq_num as u64,
                    obj_version,
                )?;
            }
            for obj in tx.output_objects.iter() {
                let obj_id = obj.id();
                let obj_version = obj.version();
                self.put_object_data(obj_id, obj_version, get_store_object(obj.clone()))?;
                self.put_object_tx_seq_num(
                    obj_id,
                    data.checkpoint_summary.sequence_number,
                    tx_seq_num as u64,
                    obj_version,
                )?;
            }

            // Get the actual set of objects that have been received -- any received
            // object will show up in the modified-at set.
            let modified_at: HashSet<_> = tx.effects.modified_at_versions().into_iter().collect();
            let possible_to_receive = tx.transaction.transaction_data().receiving_objects();
            let received_objects = possible_to_receive
                .into_iter()
                .filter(|obj_ref| modified_at.contains(&(obj_ref.0, obj_ref.1)));

            for obj_ref in received_objects {
                let obj_id = obj_ref.0;
                let obj_version = obj_ref.1;
                self.put_object_version_received(obj_id, obj_version)?;
            }
        }

        Ok(())
    }

    pub fn bulk_insert_base_objects(&self, objects: Vec<Object>) -> Result<(), HistoricalDbError> {
        let cf = self
            .db
            .cf_handle(HistoricalDbCf::BaseObjectSet.name())
            .unwrap();

        let mut batch = WriteBatch::default();
        for obj in objects {
            let key = ObjectVersionKey {
                object_id: obj.id(),
                object_version: obj.version(),
            }
            .as_bytes();
            let value = bcs::to_bytes(&get_store_object(obj)).map_err(HistoricalDbError::from)?;
            batch.put_cf(&cf, key, value);
        }

        self.db.write(batch)?;

        Ok(())
    }

    /// Iterate over all object versions in the database (except for the base object set).
    pub fn iter_object_data(
        &self,
        mode: IteratorMode,
    ) -> impl Iterator<
        Item = Result<(ObjectID, VersionNumber, StoreObjectWrapper, usize), HistoricalDbError>,
    > + '_ {
        let cf = self
            .db
            .cf_handle(HistoricalDbCf::ObjectData.name())
            .unwrap();
        self.db.iterator_cf(&cf, mode).map(|res| {
            let (k, v) = res?;
            let object_id = ObjectID::from_bytes(&k[..32]).unwrap();
            let version = VersionNumber::from(u64::from_be_bytes(k[32..40].try_into().unwrap()));
            let size = v.len();
            let data = bcs::from_bytes::<StoreObjectWrapper>(&v).unwrap();
            Ok((object_id, version, data, size))
        })
    }

    /// Get the checkpoint and transaction sequence numbers for a given transaction digest.
    pub fn get_tx_chk_seq_num(
        &self,
        tx_digest: TransactionDigest,
    ) -> Result<Option<(u64, u64)>, HistoricalDbError> {
        let cf = self
            .db
            .cf_handle(HistoricalDbCf::TxDigestSeqNum.name())
            .unwrap();

        self.db
            .get_cf(&cf, tx_digest.inner())?
            .map(|v| {
                TxChkSeqNumValue::from_bytes(&v).map(|val| (val.checkpoint_seq_num, val.tx_seq_num))
            })
            .transpose()
    }

    pub fn get_highest_object_version_lte_tx(
        &self,
        object_id: ObjectID,
        checkpoint_seq_num: u64,
        max_tx_seq_num: u64,
    ) -> Result<Option<VersionNumber>, HistoricalDbError> {
        let cf = self
            .db
            .cf_handle(HistoricalDbCf::ObjectTxSeqNum.name())
            .unwrap();

        let seek_key = ObjectTxSeqNumKey {
            object_id,
            checkpoint_seq_num,
            tx_seq_num: max_tx_seq_num,
        }
        .as_bytes();

        let mut iter = self.db.iterator_cf(
            &cf,
            IteratorMode::From(&seek_key, rocksdb::Direction::Reverse),
        );

        if let Some(Ok((k, v))) = iter.next() {
            let key = ObjectTxSeqNumKey::from_bytes(&k)?;
            // must still be the same object
            if key.object_id == object_id {
                let version = ObjectTxSeqNumValue::from_bytes(&v)?.0;
                return Ok(Some(version));
            }
        }
        if let Some((_, version)) = self.get_base_object(object_id, None)? {
            return Ok(Some(version));
        }

        Ok(None)
    }

    pub fn get_highest_object_version_lte_checkpoint(
        &self,
        object_id: ObjectID,
        checkpoint_seq_num: u64,
    ) -> Result<Option<VersionNumber>, HistoricalDbError> {
        self.get_highest_object_version_lte_tx(object_id, checkpoint_seq_num, u64::MAX)
    }

    pub fn get_highest_object_data_lte_tx(
        &self,
        object_id: ObjectID,
        checkpoint_seq_num: u64,
        max_tx_seq_num: u64,
    ) -> Result<Option<(ObjectKey, StoreObjectWrapper)>, HistoricalDbError> {
        self.get_highest_object_version_lte_tx(object_id, checkpoint_seq_num, max_tx_seq_num)?
            .map(|version| {
                self.get_object(object_id, version)?
                    .map(|obj| Ok((ObjectKey(object_id, version), obj)))
                    .unwrap_or_else(|| {
                        Err(HistoricalDbError::Inconsistent(format!(
                            "object version found but object data not found for that version: object_id={}, object_version={}, checkpoint_seq_num={}",
                            object_id, version, checkpoint_seq_num
                        )))
                    })
            })
            .transpose()
    }

    pub fn get_highest_object_data_lte_checkpoint(
        &self,
        object_id: ObjectID,
        checkpoint_seq_num: u64,
    ) -> Result<Option<(ObjectKey, StoreObjectWrapper)>, HistoricalDbError> {
        self.get_highest_object_data_lte_tx(object_id, checkpoint_seq_num, u64::MAX)
    }

    fn get_base_object(
        &self,
        object_id: ObjectID,
        version: Option<VersionNumber>,
    ) -> Result<Option<(StoreObjectWrapper, VersionNumber)>, HistoricalDbError> {
        let cf = self
            .db
            .cf_handle(HistoricalDbCf::BaseObjectSet.name())
            .unwrap();

        if let Some(version) = version {
            let key = ObjectVersionKey {
                object_id,
                object_version: version,
            }
            .as_bytes();

            let value = self.db.get_cf(&cf, key)?;
            if let Some(v) = value {
                return Ok(Some((bcs::from_bytes::<StoreObjectWrapper>(&v)?, version)));
            }
        } else {
            let prefix = object_id.into_bytes();
            let mut iter = self.db.prefix_iterator_cf(&cf, prefix);

            if let Some(Ok((k, v))) = iter.next() {
                let key = ObjectVersionKey::from_bytes(&k)?;
                if key.object_id == object_id {
                    return Ok(Some((
                        bcs::from_bytes::<StoreObjectWrapper>(&v)?,
                        key.object_version,
                    )));
                }
            }
        }

        Ok(None)
    }

    pub fn get_object(
        &self,
        object_id: ObjectID,
        version: VersionNumber,
    ) -> Result<Option<StoreObjectWrapper>, HistoricalDbError> {
        let cf = self
            .db
            .cf_handle(HistoricalDbCf::ObjectData.name())
            .unwrap();

        let key = ObjectVersionKey {
            object_id,
            object_version: version,
        }
        .as_bytes();

        let res = self
            .db
            .get_cf(&cf, key)
            .map(|v| v.map(|v| bcs::from_bytes::<StoreObjectWrapper>(&v).unwrap()))
            .map_err(HistoricalDbError::from)?;

        if res.is_some() {
            return Ok(res);
        }

        self.get_base_object(object_id, Some(version))
            .map(|o| o.map(|(obj, _)| obj))
    }

    /// Returns the object with the highest version <= the given version, if it exists.
    /// Returns Ok(None) if no such version exists.
    pub fn get_object_lt_or_eq_version(
        &self,
        object_id: ObjectID,
        version: VersionNumber,
    ) -> Result<Option<StoreObjectWrapper>, HistoricalDbError> {
        let cf = self
            .db
            .cf_handle(HistoricalDbCf::ObjectData.name())
            .unwrap();

        let seek_key = ObjectVersionKey {
            object_id,
            object_version: version,
        }
        .as_bytes();

        let mut iter = self.db.iterator_cf(
            &cf,
            IteratorMode::From(&seek_key, rocksdb::Direction::Reverse),
        );

        match iter.next() {
            Some(Ok((k, v))) => {
                let key = ObjectVersionKey::from_bytes(&k)?;
                if key.object_id == object_id {
                    return Ok(Some(bcs::from_bytes::<StoreObjectWrapper>(&v)?));
                }
            }
            Some(Err(e)) => return Err(HistoricalDbError::from(e)),
            None => {}
        }

        self.get_base_object(object_id, None)
            .map(|o| o.map(|(obj, _)| obj))
    }

    pub fn have_received_object_at_version(
        &self,
        object_id: ObjectID,
        version: VersionNumber,
    ) -> Result<bool, HistoricalDbError> {
        let cf = self
            .db
            .cf_handle(HistoricalDbCf::ObjectVersionReceived.name())
            .unwrap();

        let key = ObjectVersionKey {
            object_id,
            object_version: version,
        }
        .as_bytes();

        self.db
            .get_cf(&cf, key)
            .map(|v| v.is_some())
            .map_err(HistoricalDbError::from)
    }
}
