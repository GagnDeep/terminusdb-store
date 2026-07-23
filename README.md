# terminusdb-store, a tokio-enabled data store for triple data

[![Build Status](https://github.com/terminusdb/terminusdb-store/workflows/Build/badge.svg)](https://github.com/terminusdb/terminusdb-store/actions)
[![Crate](https://img.shields.io/crates/v/terminus-store.svg)](https://crates.io/crates/terminus-store)
[![Documentation](https://docs.rs/terminus-store/badge.svg)](https://docs.rs/terminus-store/)
[![codecov](https://codecov.io/gh/terminusdb/terminusdb-store/branch/main/graph/badge.svg)](https://codecov.io/gh/terminusdb/terminusdb-store)

## Overview
This library implements a way to store triple data - data that
consists of a subject, predicate and an object, where object can
either be some value, or a node (a string that can appear both in
subject and object position).

An example of triple data is:
````
cow says value(moo).
duck says value(quack).
cow likes node(duck).
duck hates node(cow).
````
In `cow says value(moo)`, `cow` is the subject, `says` is the
predicate, and `value(moo)` is the object.

In `cow likes node(duck)`, `cow` is the subject, `likes` is the
predicate, and `node(duck)` is the object.

terminusdb-store allows you to store a lot of such facts, and search
through them efficiently.

This library is intended as a common base for anyone who wishes to
build a database containing triple data. It makes very few assumptions
on what valid data is, only focusing on the actual storage aspect.

This library is tokio-enabled. Any i/o and locking happens through
futures, and as a result, many of the functions in this library return
futures. These futures are intended to run on a tokio runtime, and
many of them will fail outside of one. If you do not wish to use
tokio, there's a small sync wrapper in `store::sync` which embeds its
own tokio runtime, exposing a purely synchronous API.

## Usage
Add this to your `Cargo.toml`:

```toml
[dependencies]
terminus-store = "0.19.2"
```

create a directory where you want the store to be, then open that store with
```rust
let store = terminus_store::open_directory_store("/path/to/store").await.unwrap();
```

Or use the sync wrapper:
```rust
let store = terminus_store::open_sync_directory_store("/path/to/store").unwrap();
```

For more information, [visit the documentation on docs.rs](https://docs.rs/terminus-store/).

See also the `examples/` directory for some basic examples.

## Object-storage backend (optional)

An optional S3-compatible object-storage backend keeps a store inside a bucket
instead of a local directory: the bucket is the source of truth and compute is
stateless (durability, cheap storage, stateless replicas, trivial backup). It is
built on the Apache Arrow [`object_store`](https://crates.io/crates/object_store)
crate, so the same code works against S3, GCS, Azure, Cloudflare R2, MinIO, a
local filesystem, or an in-memory store.

It does **not** make the database larger than RAM — the working set is still
fully loaded and expanded in memory on read. The wins are operational.

Enable the default-off feature:

```toml
[dependencies]
terminus-store = { version = "0.21", features = ["object-store"] }
object_store = { version = "0.11", features = ["aws"] }
```

Open a store over any `object_store::ObjectStore`:

```rust
use std::sync::Arc;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};

// S3 / R2 / MinIO. `with_conditional_put(ETagMatch)` enables the label
// compare-and-swap; `with_endpoint` + `with_allow_http` target MinIO/R2.
let s3 = AmazonS3Builder::new()
    .with_endpoint("http://localhost:9100")     // omit for real AWS S3
    .with_bucket_name("terminusdb")
    .with_access_key_id("minioadmin")
    .with_secret_access_key("minioadmin")
    .with_region("us-east-1")
    .with_allow_http(true)                       // MinIO over http; drop for https
    .with_conditional_put(S3ConditionalPut::ETagMatch)
    .build()
    .unwrap();

// prefix keys under "graphs/", 100 MiB in-memory layer cache
let store = terminus_store::open_object_store(Arc::new(s3), "graphs", 100);

// or add a local-disk spill cache tier between RAM and the network:
let (store, disk_stats) = terminus_store::open_object_store_with_cache(
    Arc::new(s3), "graphs", 100, "/var/cache/terminusdb".into());
```

For a network-free store (tests, caching), point it at
`object_store::memory::InMemory` or `object_store::local::LocalFileSystem`. Note
that `LocalFileSystem` does not support conditional PUT, so label
compare-and-swap requires a store that does (S3/GCS/Azure/R2/MinIO/InMemory).

**Layout & config.** One layer is one immutable object at
`<prefix>/<first-3-hex>/<hash>.larch`; labels are compare-and-swap objects at
`<prefix>/<name>.label`. Credentials and the endpoint override for R2/MinIO are
passed through `object_store`'s builders (`AmazonS3Builder`, `GoogleCloudStorageBuilder`,
`MicrosoftAzureBuilder`) or its env vars (`AWS_ENDPOINT`, `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_ALLOW_HTTP`).

**Performance & scale.** The backend is tuned for low S3 latency and RAM:
layers are read with a parallel-prefetch stack manifest (deep-chain cold reads
drop ~13× vs. a sequential walk), archives are memory-mapped for larger-than-RAM
reads over a local-NVMe/RAM buffer pool, `Store::spawn_compaction` bounds read
depth via non-destructive rollup, and `store::buffered::BufferedNamedGraph` adds
opt-in group commit (many commits → one object) with an optional per-commit
write-ahead log. See [`docs/object-store-optimizations.md`](docs/object-store-optimizations.md).

**Integration tests.** `docker-compose.minio.yml` brings up MinIO for the
`#[ignore]`d integration tests; see the design notes in
[`docs/RFC-object-store.md`](docs/RFC-object-store.md).

## Upgrading from 0.19 or earlier
Starting with version 0.20.0, terminus-store uses a new storage format, which bundles all files into a single archive, and also supports value types. Stores created using 0.19 or earlier will not work with 0.20 or later. However, there is a conversion tool to convert existing pre-v20 stores: [terminusdb-10-to-11](https://github.com/terminusdb/terminusdb-10-to-11/).

## Roadmap

We are constantly developing terminusdb-store to make it a high quality succinct graph representation versioned datastorage layer. To help facilitate understanding of our aims for this project we have laid out a [Roadmap](./docs/ROADMAP.md). If you would like to assist in the development of terminusdb-store, or you think something should be added to the roadmap please contact us.

## License
terminus-store is licensed under Apache 2.0.

## Contributing
See [CONTRIBUTING.md](CONTRIBUTING.md)

## See also
- The Terminus database, for which this library was written: [Website](https://terminusdb.com) - [GitHub](https://github.com/terminusdb/)
- Our prolog bindings for this library: [terminus_store_prolog](https://github.com/terminusdb/terminus_store_prolog/)
- The HDT format, which the terminusdb-store layer format is based on: [Website](http://www.rdfhdt.org/)
