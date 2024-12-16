mod args;

use std::process::exit;

use anyhow::Result;
use clap::{CommandFactory, Parser};
use tracing::error;
use xlake::PipeSession;

#[global_allocator]
static ALLOC: ::snmalloc_rs::SnMalloc = ::snmalloc_rs::SnMalloc;

#[::tokio::main]
async fn main() {
    let args = self::args::Args::parse();

    let level = if args.debug { "DEBUG" } else { "INFO" };
    ::cdl_k8s_core::otel::init_once_with(level, true);

    if let Err(error) = try_main(args).await {
        error!("{error}");
        exit(1)
    }
}

async fn try_main(args: self::args::Args) -> Result<()> {
    let self::args::Args { command, debug: _ } = args;
    let input = command.join(" ");
    if input.trim().is_empty() {
        <self::args::Args as CommandFactory>::command().print_help()?;
        return Ok(());
    }

    let session = PipeSession::default();
    session.call(&input).await?;
    Ok(())
}
