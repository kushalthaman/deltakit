use anyhow::Result;
use clap::{ArgAction, Parser};
use indicatif::{ProgressBar, ProgressStyle};
use serde::Serialize;
use std::time::Duration;
use tracing::level_filters::LevelFilter;

#[derive(Debug, Clone, Parser)]
#[command(name = "deltakit", disable_version_flag = true)]
pub struct GlobalArgs {
    #[arg(long, global = true, action = ArgAction::SetTrue)]
    pub json: bool,

    #[arg(long, global = true, action = ArgAction::SetTrue)]
    pub quiet: bool,

    #[arg(long, global = true, default_value_t = true)]
    pub progress: bool,

    #[arg(long, global = true)]
    pub concurrency: Option<usize>,

    #[arg(long, global = true)]
    pub timeout: Option<String>,

    #[arg(long, global = true)]
    pub profile: Option<String>,

    #[arg(long, global = true)]
    pub role_arn: Option<String>,

    #[arg(long, global = true)]
    pub region: Option<String>,
}

pub fn init_tracing(is_quiet: bool, as_json: bool) -> Result<()> {
    let env_filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    let env_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .parse_lossy(env_filter);
    if as_json {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .json()
            .with_ansi(false)
            .with_target(false)
            .with_current_span(false)
            .with_span_list(false)
            .with_file(false)
            .with_line_number(false)
            .init();
    } else if !is_quiet {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_target(false)
            .with_file(false)
            .with_line_number(false)
            .compact()
            .init();
    }
    Ok(())
}

pub fn pb_spinner(enabled: bool, message: &str) -> Option<ProgressBar> {
    if !enabled {
        return None;
    }
    let pb = ProgressBar::new_spinner();
    pb.enable_steady_tick(Duration::from_millis(120));
    pb.set_style(
        ProgressStyle::with_template("{spinner:.green} {msg}")
            .unwrap()
            .tick_strings(&["⠁", "⠂", "⠄", "⡀", "⢀", "⠠", "⠐", "⠈"]),
    );
    pb.set_message(message.to_string());
    Some(pb)
}

pub fn print_output<T: Serialize>(json: bool, value: &T) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(value)?);
    } else {
        println!("{}", serde_json::to_string_pretty(value)?);
    }
    Ok(())
}

impl GlobalArgs {
    pub fn timeout_duration(&self) -> Option<Duration> {
        self.timeout
            .as_ref()
            .and_then(|s| humantime::parse_duration(s).ok())
    }
}