# XLake

A [_GStreamer-like_](https://gstreamer.freedesktop.org/) Workflow Framework,
supporting [_NVIDIA Omniverse_](https://www.nvidia.com/en-us/omniverse/solutions/digital-twins/), [_Python_](https://www.python.org/) and Web UI,
powered by [_K8S_](https://kubernetes.io/) & [_Rust_](https://www.rust-lang.org/).

It is under a heavy construction.
Unfinished features may have significant changes in composition and usage.
Please read the [feature support](#feature-support) below carefully.

## Feature support

### Components

| Type                   | How to read                                                                                                                            |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| Feature Kind           | e.g. **model**                                                                                                                         |
| Feature Group          | e.g. builtin/                                                                                                                          |
| Feature Name           | e.g. doc                                                                                                                               |
| Feature's Usage        | e.g. <ins>**model**</ins>/builtin/<ins>doc</ins> -> <ins>doc**model**</ins> <br> ♜ _Ignore the group names and swap the term_ 🏰 </br> |
| Model Function Name    | e.g. \:split                                                                                                                           |
| Model Function's Usage | e.g. **model**/builtin/<ins>doc\:split</ins> -> <ins>doc:split</ins>                                                                   |
| Status                 | ✅ Yes 🚧 WIP 🔎 TBA 🔲 TBD                                                                                                            |

- 🔎 **cluster** _([Parallel Computing](https://en.wikipedia.org/wiki/Parallel_computing) on [HPC](https://en.wikipedia.org/wiki/High-performance_computing))_
  - 🔎 local _(Current Process; by default)_
  - 🔲 ray _([Ray Cluster](https://www.ray.io/); Python-only)_
- 🔎 **engine** _(Scalable Cluster Management & Job Scheduling System)_
  - 🔲 [k8s](https://github.com/kube-rs/kube) _([Kubernetes](https://kubernetes.io/) for Containerized Applications, [HPC-Ready with OpenARK](https://github.com/ulagbulag/openark))_
  - 🔎 local _(Host Machine; by default)_
  - 🔲 slurm _([Slurm Workload Manager](https://slurm.schedmd.com/) for HPC)_
  - 🔲 terraform _([Terraform by HashiCorp](https://www.terraform.io/) for Cloud Providers)_
- 🚧 **format** _(Data File Format)_
  - 🔲 batch/ <i>([Data Table](<https://en.wikipedia.org/wiki/Table_(information)>), [Data Catalog](https://en.wikipedia.org/wiki/Database_catalog))</i>
    - 🔲 [delta](https://github.com/delta-io/delta-rs) _([Delta Lake](https://delta.io/))_
    - 🔲 [lance](https://github.com/lancedb/lance) _(100x faster random access than [Parquet](https://parquet.apache.org/))_
  - ✅ stream _(In-Memory, by default)_
    - ✅ Dynamic type casting
    - ✅ [Lazy Evaluation](https://en.wikipedia.org/wiki/Lazy_evaluation)
- 🚧 **model** _([Data Schema](https://en.wikipedia.org/wiki/Database_schema) & [Metadata](https://en.wikipedia.org/wiki/Metadata))_
  - 🚧 builtins/ _(Primitives)_
    - 🔲 batch _(Auto-derived by the batch format)_
      - 🔲 :sql
    - ✅ binary
    - 🔎 content
      - 🔎 :prompt _([LLM Prompt](https://openai.com/index/chatgpt/))_
    - 🚧 doc
      - 🔲 :split
    - 🔲 embed
      - 🔲 :vector_search
    - ✅ file
    - ✅ hash _(Hashable -> Storable)_
    - 🔲 metadata _(Nested, Unsafe, for additional description)_
  - 🔲 document/ _([LibreOffice](https://www.libreoffice.org/), etc.)_
    - 🔲 email
    - 🔲 markdown
    - 🔲 pdf
    - 🔲 tex
  - 🔲 media/ _([GStreamer](https://gstreamer.freedesktop.org/))_
    - 🔲 audio
    - 🔲 image
    - 🔲 video
  - 🔲 ml/ _(Machine Learning, not Artificial Intelligence)_
    - torch _([PyTorch](https://pytorch.org/))_
      - eval
      - train
  - 🔲 twin/ _([Digital Twin](https://en.wikipedia.org/wiki/Digital_twin))_
    - 🔲 loc _(Location)_
    - 🔲 rot _(Rotation)_
    - 🔲 usd _([OpenUSD](https://openusd.org/release/index.html))_
- 🚧 **sink** _(Data Visualization & Workload Automation)_
  - 🚧 local/
    - 🔲 file
    - 🔲 media _([GStreamer](https://gstreamer.freedesktop.org/))_
    - ✅ stdout
  - 🔲 twin/ _([Digital Twin](https://en.wikipedia.org/wiki/Digital_twin) & [Robotics](https://en.wikipedia.org/wiki/Robotics))_
    - 🔲 omni _([NVIDIA Omniverse](https://www.nvidia.com/en-us/omniverse/))_
- 🚧 **src** _(Data Source)_
  - 🔲 cloud/
    - 🔲 gmail _([Google Gmail](https://mail.google.com))_
  - 🔲 desktop/
    - 🔲 screen _(Screen Capture & Recording)_
  - 🚧 local/
    - 🚧 file
      - ✅ Content-based Hash
      - ✅ Lazy Evaluation
      - 🔲 Metadata-based Hash
    - ✅ stdin
  - 🔲 ml/ _(Machine Learning Models & Datasets)_
    - 🔲 huggingface _([Hugging Face Models & Datasets](https://huggingface.co/))_
    - 🔲 kaggle _([Kaggle Datasets](https://www.kaggle.com/))_
  - 🔲 monitoring/ _([Time series database](https://en.wikipedia.org/wiki/Time_series_database), etc.)_
    - 🔲 [prometheus](https://github.com/prometheus/client_rust) _([CNCF-graduated TSDB](https://mail.google.com))_
  - 🔲 rtls/ _([Real-Time Location System](https://en.wikipedia.org/wiki/Real-time_locating_system))_
    - 🔲 sewio _([Sewio UWB](https://www.sewio.net/))_
  - 🔲 twin/ _([Digital Twin](https://en.wikipedia.org/wiki/Digital_twin))_
    - 🔲 omni _([NVIDIA Omniverse](https://www.nvidia.com/en-us/omniverse/))_
- 🚧 **store** _([Object Store](https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html), Cacheable)_
  - 🔲 cdl _([Connected Data Lake](https://github.com/SmartX-Team/connected-data-lake))_
  - 🔲 cloud/
    - 🔲 gdrive _([Google Drive](https://workspace.google.com/products/drive/))_
    - 🔲 s3 _([Amazon S3](https://aws.amazon.com/ko/s3/))_
      - 🔲 [Multipart upload API](https://docs.rs/object_store/latest/object_store/multipart/trait.MultipartStore.html)
  - ✅ local _(FileSystem)_

### User Interfaces

---

| Type   | How to read                 |
| ------ | --------------------------- |
| Status | ✅ Yes 🚧 WIP 🔎 TBA 🔲 TBD |

- 🔎 API
  - 🔲 Python
  - 🔎 Rust
- 🚧 CLI
  - ✅ Command-line arguments _(GStreamer-like Inline Pipeline)_
  - 🔎 Container images
  - 🔲 YAML templates
- 🔎 Web UI
  - 🔎 [Backend](https://actix.rs/)
  - 🔲 [Frontend](https://github.com/ulagbulag/cassette)
    - 🔲 Cluster Management
    - 🔲 Dashboard
    - 🔲 Graph-based Pipeline Visualization
      - 🔲 Interactive Pipeline Composition
      - 🔲 Run & Stop
      - 🔲 Save as YAML templates
    - 🔲 Job Scheduling
    - 🔲 Storage Management
  - 🔲 [Helm Chart](https://helm.sh/)

## Requirements

### Ubuntu 24.04 or Above

```bash
# Install essentials packages
sudo apt-get update && sudo apt-get install \
  default-jre \
  libreoffice-java-common \
  rustup

# Install the latest rustc
rustup default stable
```

## Usage

### Save a File into the Storage

Change the file path and the store type into your preferred ones.

```bash
cargo run --release -- xlake "filesrc path='my_file.pdf'
  ! localstore path='my_cache_dir'
  ! stdoutsink"
```

### LLM Search on my Gmail

```bash
cargo run --release -- xlake "gmailsrc k=10
  ! localstore
  ! doc:split to=paragraph
  ! doc:embed embeddings=openai
  ! localstore
  ! embed:vector_search query='my query' k=5
  ! content:prompt prompt="Summarize the email contents in bullets"
  ! stdoutsink"
```

### Simple LLM Call

```bash
cargo run --release -- xlake "emptysrc
  ! content:prompt prompt='Which is better: coke zero vs normal coke'
  ! stdoutsink"
```

## Usage in Container Runtime (Docker, ...)

```bash
docker run --rm quay.io/ulagbulag/xlake:latest "emptysrc
  ! content:prompt prompt='Which is better: coke zero vs normal coke'
  ! stdoutsink"
```

---

#### License

<sup>
Licensed under either of <a href="LICENSE-APACHE">Apache License, Version
2.0</a> or <a href="LICENSE-MIT">MIT license</a> at your option.
</sup>

<br>

<sub>
Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in XLake by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
</sub>
