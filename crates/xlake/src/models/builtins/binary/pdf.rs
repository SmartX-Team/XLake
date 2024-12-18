use std::{fmt, path::PathBuf, process::Stdio};

use anyhow::{bail, Context, Result};
use async_tempfile::TempFile;
use async_trait::async_trait;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use tokio::{fs, io::AsyncWriteExt, process::Command};
use tracing::debug;
use which::which;
use xlake_ast::{PlanArguments, PlanKind};
use xlake_core::{
    object::LazyObject, PipeChannel, PipeEdge, PipeFunc, PipeModelOwned, PipeModelOwnedExt,
    PipeNodeFactory, PipeNodeImpl,
};

use crate::models::builtins::file::FileModelView;

use super::BinaryModelView;

#[derive(Copy, Clone, Debug, Default)]
pub struct PdfFactory;

impl fmt::Display for PdfFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind().fmt(f)
    }
}

#[async_trait]
impl PipeNodeFactory for PdfFactory {
    fn kind(&self) -> PlanKind {
        PlanKind::Func {
            model_name: super::consts::NAME.into(),
            func: self.name(),
        }
    }

    fn name(&self) -> String {
        "pdf".into()
    }

    fn input(&self) -> PipeEdge {
        PipeEdge {
            model: Some(vec![super::consts::NAME.into()]),
            ..Default::default()
        }
    }

    fn output(&self) -> PipeEdge {
        PipeEdge {
            model: Some(vec!["doc".into()]),
            ..Default::default()
        }
    }

    async fn build(&self, args: &PlanArguments) -> Result<PipeNodeImpl> {
        let mut imp: PdfFunc = args.to()?;
        imp.init().await?;
        Ok(PipeNodeImpl::Func(Box::new(imp)))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PdfFunc {
    #[serde(default = "PdfFunc::default_prog")]
    pub prog: PathBuf,
}

impl PdfFunc {
    fn default_prog() -> PathBuf {
        "libreoffice".parse().unwrap()
    }
}

impl Default for PdfFunc {
    fn default() -> Self {
        Self {
            prog: Self::default_prog(),
        }
    }
}

impl PdfFunc {
    async fn init(&mut self) -> Result<()> {
        let Self { prog } = self;
        *prog = which(&prog)?;
        Ok(())
    }

    async fn convert(&self, item: LazyObject) -> Result<LazyObject> {
        // Download the file contents
        let item = item.flatten().await?;
        let mut item: BinaryModelView = match item.view() {
            Ok(item) => item,
            Err(item) => return Ok(item),
        };

        // Save to a temporary file
        let mut src = TempFile::new().await?;
        src.write_all(item.content()).await?;

        // Convert the file into the [format]
        let format = "pdf";
        let parent = src.file_path().parent().context("Not a file")?;
        let child = Command::new(&self.prog)
            .arg("--headless")
            .arg("--invisible")
            .arg("--convert-to")
            .arg(format)
            .arg(src.file_path())
            .current_dir(parent)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        // Handle the output
        let output = child.wait_with_output().await?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        debug!("{}", stdout.trim());
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("Failed to convert the file: {}", stderr.trim())
        }

        // Load the converted file
        let dst = {
            let src = src.file_path();
            let prefix = src
                .file_stem()
                .and_then(|stem| stem.to_str())
                .context("Invalid file name")?;
            let name = format!("{prefix}.{format}");
            parent.join(name)
        };
        let content = fs::read(&dst).await?;

        // Cleanup
        fs::remove_file(dst).await.ok();
        drop(src);

        // Create a layer
        *item.content() = content;
        let item = item.__into_inner();
        let item = match item.view::<FileModelView>() {
            Ok(mut item) => {
                *item.extension() = format.into();
                item.__into_inner()
            }
            Err(item) => item,
        };
        Ok(item)
    }
}

#[async_trait]
impl PipeFunc for PdfFunc {
    async fn call(&self, channel: PipeChannel) -> Result<PipeChannel> {
        channel
            .into_stream()
            .await?
            .and_then(|item| self.convert(item))
            .try_collect()
            .await
    }
}
