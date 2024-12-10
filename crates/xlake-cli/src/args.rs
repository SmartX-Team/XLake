use clap::Parser;

#[derive(Debug, Parser)]
pub struct Args {
    pub command: Vec<String>,

    #[arg(global = true, long)]
    pub debug: bool,
}
