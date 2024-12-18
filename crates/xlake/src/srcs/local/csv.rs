use std::{fmt, path::PathBuf};

use anyhow::Result;
use async_trait::async_trait;
use datafusion::prelude::CsvReadOptions;
use serde::{Deserialize, Serialize};
use xlake_ast::{PlanArguments, PlanKind};
use xlake_core::{
    batch::{DataFusionBatch, DEFAULT_TABLE_REF},
    PipeChannel, PipeEdge, PipeNodeBuilder, PipeNodeImpl, PipeSrc,
};

#[derive(Copy, Clone, Debug, Default)]
pub struct CsvSrcBuilder;

impl fmt::Display for CsvSrcBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind().fmt(f)
    }
}

#[async_trait]
impl PipeNodeBuilder for CsvSrcBuilder {
    fn kind(&self) -> PlanKind {
        PlanKind::Src { name: self.name() }
    }

    fn name(&self) -> String {
        "csv".into()
    }

    fn output(&self) -> PipeEdge {
        PipeEdge {
            batch: "datafusion".into(),
            model: Some(vec!["batch".into(), "stream".into()]),
            ..Default::default()
        }
    }

    async fn build(&self, args: &PlanArguments) -> Result<PipeNodeImpl> {
        let imp: CsvSrc = args.to()?;
        Ok(PipeNodeImpl::Src(Box::new(imp)))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CsvSrc {
    path: PathBuf,
}

#[async_trait]
impl PipeSrc for CsvSrc {
    async fn call(&self) -> Result<PipeChannel> {
        let Self { path } = self;
        let path = path.to_string_lossy();

        let batch = DataFusionBatch::default();
        let options = CsvReadOptions::default();
        batch.register_csv(DEFAULT_TABLE_REF, path, options).await?;
        Ok(PipeChannel::from_batch(batch))
    }
}
