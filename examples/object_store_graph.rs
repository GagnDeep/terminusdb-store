//! End-to-end demo of the object-storage backend.
//!
//! Writes a small graph into an S3-compatible bucket, then reopens a fresh
//! store from the same bucket and prints the graph back — proving the bucket is
//! the source of truth and nothing is kept in-process.
//!
//! Run against MinIO (see `docker-compose.minio.yml` or the README):
//!
//! ```text
//! cargo run --example object_store_graph --features object-store
//! ```
//!
//! Configuration comes from the environment (with MinIO-friendly defaults):
//!   TDB_OBJECT_STORE_ENDPOINT         (default http://localhost:9233)
//!   TDB_OBJECT_STORE_BUCKET           (default terminusdb)
//!   TDB_OBJECT_STORE_ACCESS_KEY_ID    (default minioadmin)
//!   TDB_OBJECT_STORE_SECRET_ACCESS_KEY(default minioadmin)
//!   TDB_OBJECT_STORE_REGION           (default us-east-1)
//!   TDB_OBJECT_STORE_PREFIX           (default demo)

use std::env;
use std::io;
use std::sync::Arc;

use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use tdb_succinct::TdbDataType;
use terminus_store::*;

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let endpoint = env_or("TDB_OBJECT_STORE_ENDPOINT", "http://localhost:9233");
    let bucket = env_or("TDB_OBJECT_STORE_BUCKET", "terminusdb");
    let access = env_or("TDB_OBJECT_STORE_ACCESS_KEY_ID", "minioadmin");
    let secret = env_or("TDB_OBJECT_STORE_SECRET_ACCESS_KEY", "minioadmin");
    let region = env_or("TDB_OBJECT_STORE_REGION", "us-east-1");
    let prefix = env_or("TDB_OBJECT_STORE_PREFIX", "demo");

    let s3 = AmazonS3Builder::new()
        .with_endpoint(&endpoint)
        .with_bucket_name(&bucket)
        .with_access_key_id(&access)
        .with_secret_access_key(&secret)
        .with_region(&region)
        .with_allow_http(true) // MinIO over http; drop for https S3/R2
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .build()
        .expect("failed to build S3 store from environment");
    let s3: Arc<dyn object_store::ObjectStore> = Arc::new(s3);

    println!("→ writing graph to s3://{}/{}", bucket, prefix);
    {
        // 100 MiB in-memory layer cache
        let store = open_object_store(s3.clone(), prefix.clone(), 100);

        // open the graph if it already exists, otherwise create it
        let graph = match store.open("animals").await? {
            Some(g) => g,
            None => store.create("animals").await?,
        };

        // If there is already a head, stack a child layer on it (demonstrating
        // the parent chain across runs); otherwise start with a base layer.
        let new_layer = match graph.head().await? {
            Some(head) => {
                let builder = head.open_write().await?;
                builder.add_value_triple(ValueTriple::new_node("cow", "greets", "duck"))?;
                builder.commit().await?
            }
            None => {
                let builder = store.create_base_layer().await?;
                builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))?;
                builder.add_value_triple(ValueTriple::new_string_value("pig", "says", "oink"))?;
                builder.add_value_triple(ValueTriple::new_node("cow", "likes", "pig"))?;
                builder.commit().await?
            }
        };

        // Compare-and-swap the label to point at the new head.
        let updated = graph.set_head(&new_layer).await?;
        println!("  committed a new layer; set_head succeeded = {}", updated);
    }

    println!("→ reopening a fresh store from the same bucket…");
    let store = open_object_store(s3, prefix, 100);
    let graph = store
        .open("animals")
        .await?
        .expect("graph 'animals' should exist after reopen");
    let layer = graph
        .head()
        .await?
        .expect("graph 'animals' should have a head");

    println!("  triples at the current head:");
    for id_triple in layer.triples() {
        let triple = layer
            .id_triple_to_string(&id_triple)
            .expect("id triple should map to strings");
        let (kind, object) = match triple.object {
            ObjectType::Node(n) => ("node", String::make_entry(&n)),
            ObjectType::Value(v) => ("value", v),
        };
        println!(
            "    {} {} {} ({:?})",
            triple.subject, triple.predicate, kind, object
        );
    }

    Ok(())
}
