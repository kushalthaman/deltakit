use anyhow::Result;
use clap::{Parser, Subcommand, Args};
use cli_core::{GlobalArgs, init_tracing, print_output};
use deltakit_core as core;
use bytesize::ByteSize;

#[derive(Debug, Parser)]
#[command(name = "deltakit")]
#[command(about = "Delta Lake library in Rust", long_about = None)]
struct Cli {
    #[command(flatten)]
    globals: GlobalArgs,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Ls { uri: String },
    Diff { uri: String, #[arg(long)] from: i64, #[arg(long)] to: i64 },
    Rowcount { uri: String, #[arg(long = "by")] by: Option<String>, #[arg(long)] version: Option<i64> },
    CompactPlan { uri: String, #[arg(long, default_value = "256")] target: u64, #[arg(long = "by")] by: Option<String> },
    PartitionHealth { uri: String, #[arg(long = "by")] by: Option<String> },
    Manifest { uri: String, #[arg(long)] version: i64, #[arg(long, default_value = "trino")] format: String },
    VacuumDryRun { uri: String, #[arg(long, default_value = "7")] retention: i64 },
    Snapshot { uri: String, #[arg(long)] version: i64, #[arg(long)] out: String },
    ShardManifest { uri: String, #[arg(long)] version: i64, #[arg(long)] shards: u32, #[arg(long, default_value = "bytes")] balance: String, #[arg(long = "by")] by: Option<String>, #[arg(long = "sticky-by")] sticky_by: Option<String>, #[arg(long = "max-files-per-shard")] max_files_per_shard: Option<usize>, #[arg(long = "row-group-aware", default_value_t = false)] row_group_aware: bool },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.globals.quiet, cli.globals.json)?;

    match cli.command {
        Commands::Ls { uri } => cmd_ls(&cli.globals, &uri).await?,
        Commands::Diff { uri, from, to } => cmd_diff(&cli.globals, &uri, from, to).await?,
        Commands::Rowcount { uri, by, version } => cmd_rowcount(&cli.globals, &uri, by, version).await?,
        Commands::CompactPlan { uri, target, by } => cmd_compact_plan(&cli.globals, &uri, target, by).await?,
        Commands::PartitionHealth { uri, by } => cmd_partition_health(&cli.globals, &uri, by).await?,
        Commands::Manifest { uri, version, format } => cmd_manifest(&cli.globals, &uri, version, &format).await?,
        Commands::VacuumDryRun { uri, retention } => cmd_vacuum(&cli.globals, &uri, retention).await?,
        Commands::Snapshot { uri, version, out } => cmd_snapshot(&cli.globals, &uri, version, &out).await?,
        Commands::ShardManifest { uri, version, shards, balance, by, sticky_by, max_files_per_shard, row_group_aware } => cmd_shard_manifest(&cli.globals, &uri, version, shards, &balance, by, sticky_by, max_files_per_shard, row_group_aware).await?,
    }
    Ok(())
}

async fn cmd_ls(glob: &GlobalArgs, uri: &str) -> Result<()> {
    let h = core::load_table(uri).await?;
    let version = core::current_version(&h).await?;
    let files = core::list_active_files(&h, Some(version)).await?;
    let total_files = files.len();
    let total_bytes: i64 = files.iter().map(|f| f.size).sum();
    let partitions: Vec<String> = files.iter().flat_map(|f| f.partition_values.keys().cloned()).collect();
    let mut uniq = std::collections::BTreeSet::new();
    for p in partitions { uniq.insert(p); }
    if glob.json {
        #[derive(serde::Serialize)]
        struct LsOut { uri: String, version: i64, files: usize, bytes: i64, partitions: Vec<String> }
        let out = LsOut { uri: uri.to_string(), version, files: total_files, bytes: total_bytes, partitions: uniq.into_iter().collect() };
        print_output(true, &out)
    } else {
        println!("{}", uri);
        println!("  version: {}", version);
        println!("  files:   {}", total_files);
        println!("  size:    {} ({} B)", ByteSize(total_bytes as u64), total_bytes);
        let parts: Vec<String> = uniq.into_iter().collect();
        if !parts.is_empty() { println!("  partitions: {}", parts.join(",")); }
        Ok(())
    }
}

async fn cmd_diff(glob: &GlobalArgs, uri: &str, from: i64, to: i64) -> Result<()> {
    let h = core::load_table(uri).await?;
    let out = core::diff_versions(&h, from, to).await?;
    if glob.json { print_output(true, &out) } else {
        println!("v{}..v{}: +{} files ({}), -{} files ({})",
            out.from,
            out.to,
            out.files_added,
            ByteSize(out.bytes_added as u64),
            out.files_removed,
            ByteSize(out.bytes_removed as u64)
        );
        Ok(())
    }
}

async fn cmd_rowcount(glob: &GlobalArgs, uri: &str, by: Option<String>, version: Option<i64>) -> Result<()> {
    let h = core::load_table(uri).await?;
    let gb: Vec<String> = by.map(|s| s.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()).unwrap_or_default();
    let out = core::fast_rowcount(&h, &gb, version).await?;
    if glob.json { print_output(true, &out) } else {
        if gb.is_empty() {
            let total: u64 = out.iter().map(|r| r.rows).sum();
            println!("rows ~{}", total);
        } else {
            println!("group-by: {}", gb.join(","));
            for r in out {
                let k = r.group.into_iter().map(|(k,v)| format!("{}={}", k, v)).collect::<Vec<_>>().join(",");
                println!("{}\t{}", k, r.rows);
            }
        }
        Ok(())
    }
}

async fn cmd_compact_plan(glob: &GlobalArgs, uri: &str, target: u64, by: Option<String>) -> Result<()> {
    let h = core::load_table(uri).await?;
    let gb: Vec<String> = by.map(|s| s.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()).unwrap_or_default();
    let out = core::plan_compaction(&h, target, &gb).await?;
    if glob.json { print_output(true, &out) } else {
        println!("target: {} MB", target);
        println!("groups: {}", out.groups.len());
        println!("est. IO: {}", ByteSize(out.estimated_io_bytes));
        Ok(())
    }
}

async fn cmd_partition_health(glob: &GlobalArgs, uri: &str, by: Option<String>) -> Result<()> {
    let h = core::load_table(uri).await?;
    let gb: Vec<String> = by.map(|s| s.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()).unwrap_or_default();
    let out = core::partition_health(&h, &gb).await?;
    if glob.json { print_output(true, &out) } else {
        println!("files: {}", out.total_files);
        for c in out.cardinality { println!("{}: {}", c.key, c.distinct); }
        Ok(())
    }
}

async fn cmd_manifest(glob: &GlobalArgs, uri: &str, version: i64, format: &str) -> Result<()> {
    let h = core::load_table(uri).await?;
    let fmt = match format.to_ascii_lowercase().as_str() {
        "trino" => core::ManifestFormat::Trino,
        "hive" => core::ManifestFormat::Hive,
        "presto" => core::ManifestFormat::Presto,
        _ => core::ManifestFormat::FileList,
    };
    let out = core::generate_manifest(&h, version, fmt).await?;
    if glob.json { print_output(true, &out) } else {
        println!("version: {}", out.version);
        println!("files: {}", out.files.len());
        Ok(())
    }
}

async fn cmd_vacuum(glob: &GlobalArgs, uri: &str, retention: i64) -> Result<()> {
    let h = core::load_table(uri).await?;
    let out = core::vacuum_dry_run(&h, retention).await?;
    if glob.json { print_output(true, &out) } else {
        println!("referenced: {}", out.referenced_files);
        println!("existing:   {}", out.existing_files);
        println!("orphans:    {}", out.orphans);
        println!("safe:       {}", out.safe);
        Ok(())
    }
}

async fn cmd_snapshot(_glob: &GlobalArgs, uri: &str, version: i64, out: &str) -> Result<()> {
    let h = core::load_table(uri).await?;
    let manifest = core::generate_manifest(&h, version, core::ManifestFormat::FileList).await?;
    let mut file = std::fs::File::create(out)?;
    use std::io::Write;
    for e in manifest.files {
        writeln!(file, "{}", e.path)?;
    }
    Ok(())
}

async fn cmd_shard_manifest(glob: &GlobalArgs, uri: &str, version: i64, shards: u32, balance: &str, by: Option<String>, sticky_by: Option<String>, max_files: Option<usize>, row_group_aware: bool) -> Result<()> {
    use shard_planner as sp;
    let mode = match balance.to_ascii_lowercase().as_str() { "rows" => sp::BalanceMode::Rows, _ => sp::BalanceMode::Bytes };
    let split_csv = |s: Option<String>| -> Vec<String> { s.map(|x| x.split(',').map(|t| t.trim().to_string()).filter(|t| !t.is_empty()).collect()).unwrap_or_default() };
    let opts = sp::ShardOptions { by: split_csv(by), sticky_by: split_csv(sticky_by), max_files_per_shard: max_files, balance: mode, row_group_aware };
    let h = core::load_table(uri).await?;
    let shards = sp::plan_shards(&h, version, shards, opts).await?;
    print_output(glob.json, &shards)
}


