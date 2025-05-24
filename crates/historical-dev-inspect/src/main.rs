pub mod db;
pub mod execution;
pub mod init;
pub mod rpc;
pub mod store;
pub mod transaction_input_loader;
pub mod checkpoint_store;

use clap::{Parser, Subcommand};
use init::{initialize_base_object_set_from_snapshot, load_checkpoints_into_db};
use object_store::DynObjectStore;
use std::{path::PathBuf, sync::Arc};
use sui_config::object_storage_config::{ObjectStoreConfig, ObjectStoreType};
use sui_data_ingestion_core::create_remote_store_client;

use crate::db::HistoricalDb;
use crate::init::download_formal_snapshot;

#[derive(Parser, Debug)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the RPC server
    StartRpc {
        /// Path to the historical database
        #[arg(short, long)]
        db_path: String,
        /// Port to listen on
        #[arg(short, long, default_value = "8000")]
        port: u16,
    },
    /// Initialize the database
    InitializeDb {
        /// Path to the database
        #[arg(short, long)]
        db_path: String,
        /// The starting epoch number for database initialization (inclusive)
        #[arg(short = 's', long)]
        start_epoch: u64,
        /// The final checkpoint sequence number to process during initialization (inclusive)
        #[arg(short = 'e', long)]
        end_checkpoint: u64,
        /// The directory where the base object set snapshot will be downloaded to
        #[arg(short = 't', long)]
        snapshot_dir: String,
        /// The number of threads to use for the initialization
        #[arg(short = 'c', long, default_value = "50")]
        concurrency: usize,
    },
    /// Extend the database with more checkpoints
    ExtendDb {
        /// Path to the database
        #[arg(short, long)]
        db_path: String,
        /// The final checkpoint sequence number to process during initialization (inclusive)
        #[arg(short = 'e', long)]
        end_checkpoint: u64,
        /// The number of threads to use for downloading checkpoints
        #[arg(short = 'c', long, default_value = "50")]
        concurrency: usize,
    },
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Cli::parse();

    match args.command {
        Commands::StartRpc { db_path, port } => {
            let app_url = format!("0.0.0.0:{}", port);

            let db = HistoricalDb::open(PathBuf::from(db_path)).unwrap();
            let state = rpc::AppState { db: Arc::new(db) };

            let listener = tokio::net::TcpListener::bind(&app_url).await.unwrap();

            let app = rpc::create_router().with_state(state);

            println!("Server is listening on {}", app_url);
            axum::serve(listener, app).await.unwrap();

            Ok(())
        }
        Commands::InitializeDb {
            db_path,
            start_epoch,
            end_checkpoint,
            snapshot_dir,
            concurrency,
        } => {
            println!(
                "Initializing database at {} from epoch {} to checkpoint {}",
                db_path, start_epoch, end_checkpoint
            );

            if start_epoch == 0 {
                return Err(anyhow::anyhow!("Starting epoch must be greater than 0"));
            }

            let db = HistoricalDb::open(PathBuf::from(db_path))?;

            let m = indicatif::MultiProgress::new();

            let base_epoch = db.get_base_epoch()?;

            if let Some(base_epoch) = base_epoch {
                if base_epoch != start_epoch {
                    return Err(anyhow::anyhow!(
                        "Database already initialized with base epoch: {}, but start epoch is {}",
                        base_epoch,
                        start_epoch
                    ));
                }
            }

            if base_epoch.is_none() && start_epoch > 1 {
                println!("Downloading base epoch snapshot");
                let base_epoch = start_epoch - 1;

                println!(
                    "Initializing base object set from epoch {} snapshot",
                    base_epoch
                );
                download_formal_snapshot(&snapshot_dir, base_epoch, concurrency).await?;

                let snapshot_store_config = ObjectStoreConfig {
                    object_store: Some(ObjectStoreType::File),
                    directory: Some(PathBuf::from(snapshot_dir)),
                    ..Default::default()
                };
                let snapshot_store: Arc<DynObjectStore> =
                    snapshot_store_config.make().map(Arc::new)?;

                initialize_base_object_set_from_snapshot(
                    &db,
                    &snapshot_store,
                    base_epoch,
                    concurrency,
                    m.clone(),
                )
                .await?;
            }
            if start_epoch == 1 {
                db.set_base_epoch(0)?;
            }

            let checkpoint_store = Arc::new(create_remote_store_client(
                "https://checkpoints.mainnet.sui.io".to_string(),
                vec![],
                60,
            )?);

            load_checkpoints_into_db(
                &db,
                checkpoint_store.clone(),
                end_checkpoint,
                concurrency,
                m.clone(),
            )
            .await?;

            Ok(())
        }
        Commands::ExtendDb {
            db_path,
            end_checkpoint,
            concurrency,
        } => {
            let db = HistoricalDb::open(PathBuf::from(db_path))?;

            let checkpoint_store = Arc::new(create_remote_store_client(
                "https://checkpoints.mainnet.sui.io".to_string(),
                vec![],
                60,
            )?);

            let m = indicatif::MultiProgress::new();

            load_checkpoints_into_db(
                &db,
                checkpoint_store.clone(),
                end_checkpoint,
                concurrency,
                m.clone(),
            )
            .await?;

            Ok(())
        }
    }
}
