use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::{num::NonZeroUsize, path::PathBuf};

use anyhow::{anyhow, Result};
use byteorder::{BigEndian, ReadBytesExt};
use fastcrypto::hash::{HashFunction, Sha3_256};
use futures::TryStreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use object_store::path::Path;
use object_store::{DynObjectStore, ObjectStore};
use sui_config::object_storage_config::{ObjectStoreConfig, ObjectStoreType};
use sui_indexer_alt_framework::task::TrySpawnStreamExt;
use sui_snapshot::reader::LiveObjectIter;
use sui_snapshot::{FileMetadata, FileType, Manifest};
use sui_storage::blob::Blob;
use sui_storage::object_store::http::HttpDownloaderBuilder;
use sui_storage::object_store::util::copy_files;
use sui_storage::object_store::{ObjectStoreGetExt, ObjectStoreListExt, ObjectStorePutExt};
use sui_storage::SHA3_BYTES;
use sui_types::full_checkpoint_content::CheckpointData;
use tokio::sync::Mutex;
use tracing::info;

use crate::db::HistoricalDb;
use crate::checkpoint_store::CheckpointStoreReader;

const MANIFEST_FILE_MAGIC: u32 = 0x00C0FFEE;
const MAGIC_BYTES: usize = 4;

pub async fn initialize_base_object_set_from_snapshot(
    historical_db: &HistoricalDb,
    snapshot_store: &Arc<DynObjectStore>,
    epoch: u64,
    concurrency: usize,
    m: MultiProgress,
) -> Result<()> {
    if historical_db.get_base_epoch()?.is_some() {
        return Err(anyhow!("Base epoch already initialized"));
    }

    let epoch_dir = Path::from(format!("epoch_{}", epoch));
    let manifest_file_path = epoch_dir.child("MANIFEST");
    let manifest_bytes = snapshot_store.get_bytes(&manifest_file_path).await?;
    let manifest = read_snapshot_manifest(&manifest_bytes)?;

    let object_metadatas: Vec<FileMetadata> = manifest
        .file_metadata()
        .iter()
        .filter(|metadata| metadata.file_type == FileType::Object)
        .cloned()
        .collect();

    let obj_progress_bar = m.add(ProgressBar::new(object_metadatas.len() as u64).with_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] {wide_bar} {pos}/{len} .obj files processed ({percent}%) - {msg}",
        ).unwrap(),
    ));
    obj_progress_bar.set_message("processing .obj files");

    futures::stream::iter(object_metadatas)
        .try_for_each_spawned(concurrency, |file_meta| {
            let file_path = file_meta.file_path(&epoch_dir);
            let snapshot_store = snapshot_store.clone();
            let historical_db = historical_db.clone();
            let obj_progress_bar = obj_progress_bar.clone();

            async move {
                let file_bytes = snapshot_store.get_bytes(&file_path).await?;
                let objects: Vec<_> = LiveObjectIter::new(&file_meta, file_bytes)?
                    .filter_map(|live_obj| live_obj.to_normal())
                    .collect();
                historical_db.bulk_insert_base_objects(objects)?;

                obj_progress_bar.inc(1);
                obj_progress_bar.set_message(format!("file: {}", file_path));
                Ok::<_, anyhow::Error>(())
            }
        })
        .await?;

    obj_progress_bar.finish_with_message("Finished processing .obj files");

    historical_db.set_base_epoch(epoch)?;

    Ok(())
}

/// Downloads a formal snapshot from the public bucket and stores it locally.
///
/// # Arguments
///
/// * `snapshot_dir` - The local directory where the snapshot will be stored
/// * `epoch` - The epoch number to download the snapshot for
///
/// # Example
///
/// ```
/// let snapshot_dir = "/path/to/snapshot/dir";
/// let epoch = 728;
/// download_formal_snapshot(snapshot_dir, epoch).await?;
/// ```
/// This will will download the snapshot to `/path/to/snapshot/dir/epoch_728`
///
pub async fn download_formal_snapshot(
    snapshot_dir: &str,
    epoch: u64,
    concurrency: usize,
) -> Result<()> {
    let snapshot_store_config = ObjectStoreConfig {
        object_store: Some(ObjectStoreType::S3),
        bucket: None,
        aws_access_key_id: None,
        aws_secret_access_key: None,
        aws_region: None,
        aws_endpoint: Some("https://formal-snapshot.mainnet.sui.io".to_string()),
        aws_virtual_hosted_style_request: true,
        object_store_connection_limit: 200,
        no_sign_request: true,
        ..Default::default()
    };
    let snapshot_store = snapshot_store_config.make_http()?;

    let local_store_config = ObjectStoreConfig {
        object_store: Some(ObjectStoreType::File),
        directory: Some(PathBuf::from(snapshot_dir)),
        ..Default::default()
    };
    let local_store: Arc<DynObjectStore> = local_store_config.make().map(Arc::new)?;

    let epoch_dir = format!("epoch_{}", epoch);
    let manifest_file_path = Path::from(epoch_dir.clone()).child("MANIFEST");

    info!("Copying snapshot manifest to local store");
    let manifest_bytes = snapshot_store.get_bytes(&manifest_file_path).await?;
    let manifest = read_snapshot_manifest(&manifest_bytes)?;

    local_store
        .put_bytes(&manifest_file_path, manifest_bytes)
        .await?;

    let epoch_dir_path = Path::from(epoch_dir.clone());

    // Filter files that need to be downloaded
    let files_to_download = {
        let mut list_stream = local_store.list_objects(Some(&epoch_dir_path)).await;
        let mut existing_files = std::collections::HashSet::new();
        while let Some(meta) = list_stream.try_next().await? {
            existing_files.insert(meta.location);
        }
        let mut missing_files = Vec::new();
        for file_metadata in manifest.file_metadata() {
            let file_path = file_metadata.file_path(&epoch_dir_path);
            if !existing_files.contains(&file_path) {
                missing_files.push(file_path);
            }
        }
        missing_files
    };

    info!(
        "Snapshot download status: {}/{} files already exist locally ({}% complete), {} files to download",
        manifest.file_metadata().len() - files_to_download.len(),
        manifest.file_metadata().len(),
        ((manifest.file_metadata().len() - files_to_download.len()) * 100)
            / manifest.file_metadata().len(),
        files_to_download.len()
    );

    let m = MultiProgress::new();
    let progress_bar = m.add(
        ProgressBar::new(files_to_download.len() as u64).with_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] {wide_bar} {pos}/{len} files downloaded ({percent}%) - {msg}",
            )
            .unwrap(),
        ),
    );
    progress_bar.set_message("starting download");

    copy_files(
        &files_to_download,
        &files_to_download,
        &snapshot_store,
        &local_store,
        NonZeroUsize::new(concurrency).unwrap(),
        Some(progress_bar.clone()),
    )
    .await?;

    Ok(())
}

/// Reads and validates a manifest from a byte array.
///
/// This function parses a manifest file from raw bytes, performing several validation steps:
/// 1. Checks if the data is long enough to contain the magic bytes and SHA3 digest
/// 2. Verifies the magic bytes match the expected manifest file magic number
/// 3. Validates the SHA3 checksum of the content
/// 4. Deserializes the manifest content using BCS
///
/// # Arguments
/// * `bytes` - The raw bytes containing the manifest file data
///
/// # Returns
/// A Result containing either:
/// * Ok(Manifest) - The successfully parsed manifest
/// * Err(anyhow::Error) - An error if validation fails or parsing fails
pub fn read_snapshot_manifest(bytes: &[u8]) -> anyhow::Result<Manifest> {
    if bytes.len() < MAGIC_BYTES + SHA3_BYTES {
        return Err(anyhow!("Manifest data too short"));
    }

    let mut reader = std::io::Cursor::new(bytes);
    let magic = reader.read_u32::<BigEndian>()?;
    if magic != MANIFEST_FILE_MAGIC {
        return Err(anyhow!("Unexpected magic byte: {}", magic));
    }

    // Get the content and digest parts
    let content_end = bytes.len() - SHA3_BYTES;
    let content = &bytes[MAGIC_BYTES..content_end];
    let sha3_digest = &bytes[content_end..];

    // Verify checksum
    let mut hasher = Sha3_256::default();
    hasher.update(&bytes[..content_end]);
    let computed_digest = hasher.finalize().digest;
    if computed_digest != sha3_digest {
        return Err(anyhow!(
            "Checksum: {:?} don't match: {:?}",
            computed_digest,
            sha3_digest
        ));
    }

    let manifest = bcs::from_bytes(content)?;
    Ok(manifest)
}

/// Verifies the integrity of a snapshot by checking file digests against the manifest.
///
/// This function compares the SHA3 digests of existing files in the local store against
/// the digests stored in the manifest. It identifies files that are either missing or
/// have different digests than expected.
///
/// # Arguments
/// * `manifest` - The snapshot manifest containing file metadata and expected digests
/// * `store` - The object store containing the snapshot files
/// * `epoch` - The epoch number of the snapshot
/// * `m` - A MultiProgress instance for displaying progress bars
/// * `concurrency` - The number of concurrent verification tasks
///
/// # Returns
/// A Result containing a Vec of Paths for files that are either missing or invalid
pub async fn verify_snapshot(
    manifest: &Manifest,
    store: &Arc<DynObjectStore>,
    epoch: u64,
    m: MultiProgress,
    concurrency: usize,
) -> Result<Vec<Path>> {
    let epoch_dir = format!("epoch_{}", epoch);
    let epoch_dir_path = Path::from(epoch_dir.clone());

    // Get list of existing files in local store
    let mut existing_files = std::collections::HashSet::new();
    let mut list_stream = store.list_objects(Some(&epoch_dir_path)).await;
    while let Some(meta) = list_stream.try_next().await? {
        existing_files.insert(meta.location);
    }

    // First identify which files need verification and which need download
    let mut files_to_verify = Vec::new();
    let mut files_to_download = Vec::new();
    for file_metadata in manifest.file_metadata() {
        let file_path = file_metadata.file_path(&epoch_dir_path);
        if existing_files.contains(&file_path) {
            // File exists, needs verification
            files_to_verify.push((file_path, file_metadata.clone()));
        } else {
            // File doesn't exist, needs to be downloaded
            files_to_download.push(file_path);
        }
    }

    let missing_or_invalid = Arc::new(Mutex::new(files_to_download));
    let progress_bar = m.add(
        ProgressBar::new(files_to_verify.len() as u64).with_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] {wide_bar} {pos}/{len} file digests verified ({percent}%) - {msg}",
            )
            .unwrap(),
        ),
    );

    // Only verify files that exist
    futures::stream::iter(files_to_verify)
        .try_for_each_spawned(concurrency, |(file_path, file_metadata)| {
            let store = store.clone();
            let missing_or_invalid = missing_or_invalid.clone();
            let progress_bar = progress_bar.clone();

            async move {
                // File exists, check its digest
                let bytes = store.get_bytes(&file_path).await?;

                // Calculate SHA3 digest of the file
                let mut hasher = Sha3_256::default();
                hasher.update(&bytes);
                let computed_digest = hasher.finalize().digest;

                // Compare with manifest digest
                if computed_digest != file_metadata.sha3_digest {
                    missing_or_invalid.lock().await.push(file_path.clone());
                    progress_bar.set_message(format!("File {} is different", &file_path));
                } else {
                    progress_bar.set_message(format!("File {} is valid", &file_path));
                }
                progress_bar.inc(1);
                Ok::<_, anyhow::Error>(())
            }
        })
        .await?;

    progress_bar.finish_with_message("Snapshot verification complete");

    let missing_or_invalid = Arc::try_unwrap(missing_or_invalid)
        .expect("Failed to unwrap Arc")
        .into_inner();

    Ok(missing_or_invalid)
}

/// Finds the first checkpoint sequence number in the given epoch.
///
/// This function performs a binary search over checkpoint sequence numbers to find
/// the earliest checkpoint that belongs to the specified epoch. It first does an
/// exponential search to find an upper bound, then uses binary search to find the
/// exact first checkpoint. The search is performed using the public full checkpoint
/// endpoint at https://checkpoints.mainnet.sui.io.
///
/// # Arguments
///
/// * `epoch_id` - The epoch number to find the first checkpoint for
///
/// # Returns
///
/// * `Result<u64>` - The sequence number of the first checkpoint in the epoch, or an error
///   if the search fails
pub async fn find_first_checkpoint_in_epoch(
    checkpoint_store: Arc<Box<dyn ObjectStore>>,
    epoch_id: u64,
) -> Result<u64> {
    if epoch_id == 0 {
        return Err(anyhow!("Invalid epoch_id: epochs start at 1"));
    }

    // 1. Exponential search to find high where epoch(high) >= epoch_id
    let mut low: u64 = 1;
    let mut high: u64 = 1;

    while let Some(bytes) = checkpoint_store.get_checkpoint(high).await? {
        let current_epoch = Blob::from_bytes::<CheckpointData>(&bytes)?
            .checkpoint_summary
            .epoch;
        if current_epoch >= epoch_id {
            break;
        }
        // Move lower bound up and double high
        low = high + 1;
        high = high.saturating_mul(2);
        if high == 0 {
            // overflow
            high = u64::MAX;
            break;
        }
    }

    // 2. Binary search in [low..high] for first with epoch >= epoch_id
    let mut result: Option<u64> = None;
    let mut left = low;
    let mut right = high;

    while left <= right {
        let mid = left + (right - left) / 2;
        match checkpoint_store.get_checkpoint(mid).await? {
            Some(bytes) => {
                let current_epoch = Blob::from_bytes::<CheckpointData>(&bytes)?
                    .checkpoint_summary
                    .epoch;
                if current_epoch < epoch_id {
                    left = mid + 1;
                } else {
                    // current_epoch >= epoch_id
                    result = Some(mid);
                    if mid == 0 {
                        break;
                    }
                    right = mid.saturating_sub(1);
                }
            }
            None => {
                // mid beyond last checkpoint, move left
                if mid == 0 {
                    break;
                }
                right = mid.saturating_sub(1);
            }
        }
    }

    // Verify we found a checkpoint and it belongs exactly to epoch_id
    if let Some(first) = result {
        if let Some(bytes) = checkpoint_store.get_checkpoint(first).await? {
            let e = Blob::from_bytes::<CheckpointData>(&bytes)?
                .checkpoint_summary
                .epoch;
            if e == epoch_id {
                return Ok(first);
            }
        }
    }

    Err(anyhow!("Epoch {} not found in checkpoints", epoch_id))
}

/// Loads checkpoints from a checkpoint store into the historical database.
///
/// This function iterates through checkpoints in the given store, starting from the current
/// database watermark (or the first checkpoint in the next epoch if no watermark is present),
/// and loads them into the historical database up to and including the specified `end_checkpoint`.
/// Progress is displayed using the provided [`MultiProgress`] instance.
///
/// # Arguments
///
/// * `db` - Reference to the [`HistoricalDb`] where checkpoints will be loaded.
/// * `checkpoint_store` - An [`Arc`] pointing to a boxed [`ObjectStore`] containing checkpoint data.
/// * `end_checkpoint` - The last checkpoint sequence number (inclusive) to load.
/// * `m` - An [`indicatif::MultiProgress`] instance for displaying progress bars.
///
/// # Returns
///
/// Returns `Ok(())` if all checkpoints are loaded successfully, or an error if any step fails.
///
pub async fn load_checkpoints_into_db(
    db: &HistoricalDb,
    checkpoint_store: Arc<Box<dyn ObjectStore>>,
    end_checkpoint: u64,
    concurrency: usize,
    m: MultiProgress,
) -> Result<()> {
    let base_epoch = db
        .get_base_epoch()?
        .ok_or_else(|| anyhow!("Base epoch not loaded into database"))?;

    let start_checkpoint = match db.get_checkpoint_watermark()? {
        Some(watermark) => watermark + 1,
        None => {
            let epoch = base_epoch + 1;
            println!(
                "No checkpoint watermark found, finding first checkpoint in epoch {}",
                epoch
            );
            let first_checkpoint =
                find_first_checkpoint_in_epoch(checkpoint_store.clone(), epoch).await?;
            println!(
                "First checkpoint in epoch {} is {}",
                epoch, first_checkpoint
            );
            first_checkpoint
        }
    };

    if start_checkpoint > end_checkpoint {
        println!(
            "No new checkpoints to load, current checkpoint watermark is >= specified end checkpoint {}",
            end_checkpoint
        );
        return Ok(());
    }

    println!(
        "Loading checkpoints from {} to {}",
        start_checkpoint, end_checkpoint
    );

    let progress_bar = m.add(
        ProgressBar::new(end_checkpoint - start_checkpoint + 1).with_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] {wide_bar} {pos}/{len} checkpoints processed ({percent}%) - {msg}",
            )
            .unwrap(),
        ),
    );
    progress_bar.set_message("loading checkpoints".to_string());

    let slot_pool = Arc::new(Mutex::new((0..concurrency).collect::<Vec<usize>>()));

    // last-completed checkpoint for every slot
    let processed: Arc<[AtomicU64]> = (0..concurrency)
        .map(|_| AtomicU64::new(start_checkpoint - 1))
        .collect::<Vec<_>>()
        .into();

    // TODO: this can be made much faster by using batch writes
    futures::stream::iter(start_checkpoint..=end_checkpoint)
        .try_for_each_spawned(concurrency, move |seq| {
            let reader = checkpoint_store.clone();
            let db = db.clone();
            let progress_bar = progress_bar.clone();
            let slot_pool = slot_pool.clone();
            let processed = processed.clone();

            async move {
                let slot_id = {
                    let mut p = slot_pool.lock().await;
                    p.pop().expect("slot_pool is empty")
                };

                let raw = reader
                    .get_checkpoint(seq)
                    .await?
                    .ok_or_else(|| anyhow!("Checkpoint {} not found", seq))?;

                let checkpoint = Blob::from_bytes::<CheckpointData>(&raw)?;
                db.process_checkpoint(&checkpoint)?;
                progress_bar.inc(1);
                progress_bar.set_message(format!("Checkpoint {}", seq));

                processed[slot_id].store(seq, Ordering::Relaxed);

                if slot_id == 0 {
                    let mut min = u64::MAX;
                    for slot in processed.iter() {
                        let v = slot.load(Ordering::Acquire);
                        min = min.min(v);
                    }
                    db.set_checkpoint_watermark(min)?;
                }

                {
                    let mut p = slot_pool.lock().await;
                    p.push(slot_id);
                }

                Ok::<_, anyhow::Error>(())
            }
        })
        .await?;

    db.set_checkpoint_watermark(end_checkpoint)?;

    Ok(())
}
