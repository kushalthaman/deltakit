use anyhow::{anyhow, Result};
use blake3::Hasher;
use deltalake::{DeltaTable, DeltaTableBuilder};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

use storage::{object_path_from_url, parse_uri, make_object_store, StorageOptions};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaTableHandle {
    pub uri: String,
    pub version: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddFileLite {
    pub path: String,
    pub size: i64,
    pub partition_values: BTreeMap<String, Option<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowCount {
    pub group: BTreeMap<String, String>,
    pub rows: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionGroup {
    pub partition: BTreeMap<String, String>,
    pub input_files: Vec<AddFileLite>,
    pub total_input_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionPlan {
    pub target_file_size_bytes: u64,
    pub partition_by: Vec<String>,
    pub groups: Vec<CompactionGroup>,
    pub estimated_io_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionCardinality {
    pub key: String,
    pub distinct: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionReport {
    pub by: Vec<String>,
    pub cardinality: Vec<PartitionCardinality>,
    pub empty_partitions: usize,
    pub total_files: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiffReport {
    pub from: i64,
    pub to: i64,
    pub files_added: usize,
    pub files_removed: usize,
    pub bytes_added: i64,
    pub bytes_removed: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry { pub path: String, pub size: i64 }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest { pub version: i64, pub files: Vec<ManifestEntry> }

pub async fn load_table(uri: &str) -> Result<DeltaTableHandle> {
    Ok(DeltaTableHandle { uri: uri.to_string(), version: None })
}

async fn load_delta(uri: &str, version: Option<i64>) -> Result<DeltaTable> {
    let parsed = parse_uri(uri)?;
    let uri_norm = parsed.url.as_str();
    let builder = if let Some(v) = version {
        DeltaTableBuilder::from_uri(uri_norm).with_version(v as i64)
    } else {
        DeltaTableBuilder::from_uri(uri_norm)
    };
    let table = builder.load().await?;
    Ok(table)
}

pub async fn current_version(h: &DeltaTableHandle) -> Result<i64> {
    let table = load_delta(&h.uri, None).await?;
    Ok(table.version())
}

pub async fn list_active_files(h: &DeltaTableHandle, version: Option<i64>) -> Result<Vec<AddFileLite>> {
    let parsed = parse_uri(&h.uri)?;
    let store = make_object_store(&h.uri, &StorageOptions::default()).await?;
    let root = storage::object_path_from_url(&parsed.url);
    let log_prefix = root.child("_delta_log");
    let mut logs = storage::list_recursively(store.clone(), &log_prefix).await?;
    logs.retain(|m| m.location.as_ref().ends_with(".json"));
    logs.sort_by_key(|m| m.location.clone());
    let target_v: Option<i64> = version;
    use std::collections::{HashMap, HashSet};
    let mut active: HashSet<String> = HashSet::new();
    let mut parts_map: HashMap<String, BTreeMap<String, Option<String>>> = HashMap::new();
    let mut size_map: HashMap<String, i64> = HashMap::new();
    for m in logs {
        let name = m.location.filename().unwrap_or("");
        if let Some(stripped) = name.strip_suffix(".json") {
            if let Ok(v) = stripped.parse::<i64>() {
                if let Some(t) = target_v { if v > t { break; } }
            }
        }
        let bytes = store.get(&m.location).await?.bytes().await?;
        for line in bytes.split(|b| *b == b'\n') {
            if line.is_empty() { continue; }
            if let Ok(val) = serde_json::from_slice::<serde_json::Value>(line) {
                if let Some(obj) = val.get("add").and_then(|v| v.as_object()) {
                    if let Some(path) = obj.get("path").and_then(|v| v.as_str()) {
                        let path_s = path.to_string();
                        active.insert(path_s.clone());
                        let mut pm = BTreeMap::new();
                        if let Some(pv) = obj.get("partitionValues").and_then(|v| v.as_object()) {
                            for (k, v) in pv {
                                pm.insert(k.clone(), v.as_str().map(|s| s.to_string()));
                            }
                        }
                        parts_map.insert(path_s.clone(), pm);
                        if let Some(sz) = obj.get("size").and_then(|v| v.as_i64()) { size_map.insert(path_s.clone(), sz); }
                    }
                } else if let Some(obj) = val.get("remove").and_then(|v| v.as_object()) {
                    if let Some(path) = obj.get("path").and_then(|v| v.as_str()) {
                        active.remove(path);
                        parts_map.remove(path);
                        size_map.remove(path);
                    }
                }
            }
        }
    }
    let mut out = Vec::with_capacity(active.len());
    for p in active.into_iter() {
        let key = root.child(p.as_str());
        let size = size_map.get(&p).copied().unwrap_or_else(|| {
            futures::executor::block_on(async { store.head(&key).await.map(|m| m.size as i64).unwrap_or(0) })
        });
        out.push(AddFileLite { path: p.clone(), size, partition_values: parts_map.remove(&p).unwrap_or_default() });
    }
    out.sort_by(|a,b| a.path.cmp(&b.path));
    Ok(out)
}

pub async fn compute_integrity_hash(h: &DeltaTableHandle, version: Option<i64>) -> Result<String> {
    let files = list_active_files(h, version).await?;
    let mut hasher = Hasher::new();
    for f in &files {
        hasher.update(f.path.as_bytes());
        hasher.update(&f.size.to_le_bytes());
        for (k, v) in &f.partition_values {
            hasher.update(k.as_bytes());
            if let Some(vs) = v { hasher.update(vs.as_bytes()); }
        }
    }
    Ok(hasher.finalize().to_hex().to_string())
}

pub async fn fast_rowcount(
    h: &DeltaTableHandle,
    group_by: &[String],
    version: Option<i64>,
) -> Result<Vec<RowCount>> {
    let add_files = list_active_files(h, version).await?;
    let mut map: HashMap<Vec<(String, String)>, u64> = HashMap::new();
    for f in add_files {
        let row_count = 0u64;
        let key = group_by
            .iter()
            .map(|k| {
                let v = f
                    .partition_values
                    .get(k)
                    .and_then(|o| o.clone())
                    .unwrap_or_else(|| "__UNKNOWN__".to_string());
                (k.clone(), v)
            })
            .collect::<Vec<_>>();
        *map.entry(key).or_insert(0) += row_count;
    }
    let mut out: Vec<RowCount> = map
        .into_iter()
        .map(|(k, v)| RowCount {
            group: k.into_iter().collect(),
            rows: v,
        })
        .collect();
    out.sort_by_key(|r| r.group.clone());
    Ok(out)
}

fn extract_partitions_from_path(path: &str) -> BTreeMap<String, Option<String>> {
    let mut map = BTreeMap::new();
    for seg in path.split('/') {
        if let Some((k, v)) = seg.split_once('=') {
            if !k.is_empty() && !v.is_empty() {
                map.insert(k.to_string(), Some(v.to_string()));
            }
        }
    }
    map
}

pub async fn plan_compaction(
    h: &DeltaTableHandle,
    target_mb: u64,
    by: &[String],
) -> Result<CompactionPlan> {
    let target = target_mb * 1024 * 1024;
    let files = list_active_files(h, None).await?;
    let mut groups: BTreeMap<Vec<(String, String)>, Vec<AddFileLite>> = BTreeMap::new();
    for f in files {
        let key = by
            .iter()
            .map(|k| {
                let v = f
                    .partition_values
                    .get(k)
                    .and_then(|o| o.clone())
                    .unwrap_or_else(|| "__UNKNOWN__".to_string());
                (k.clone(), v)
            })
            .collect::<Vec<_>>();
        groups.entry(key).or_default().push(f);
    }

    let mut plan_groups = Vec::new();
    let mut total_io: u64 = 0;
    for (k, mut files) in groups.into_iter() {
        files.sort_by_key(|f| f.size);
        let mut bucket: Vec<AddFileLite> = Vec::new();
        let mut bucket_bytes: u64 = 0;
        let mut emit = |bucket: &mut Vec<AddFileLite>, bucket_bytes: &mut u64| {
            if bucket.len() >= 2 {
                let partition_map = k.iter().cloned().collect::<BTreeMap<_, _>>();
                let grp = CompactionGroup {
                    partition: partition_map,
                    total_input_bytes: *bucket_bytes,
                    input_files: std::mem::take(bucket),
                };
                total_io += grp.total_input_bytes;
                plan_groups.push(grp);
            }
            *bucket_bytes = 0;
        };
        for f in files.into_iter() {
            let size = f.size.max(0) as u64;
            if bucket_bytes + size > target && !bucket.is_empty() {
                emit(&mut bucket, &mut bucket_bytes);
            }
            bucket_bytes += size;
            bucket.push(f);
        }
        emit(&mut bucket, &mut bucket_bytes);
    }

    Ok(CompactionPlan {
        target_file_size_bytes: target,
        partition_by: by.to_vec(),
        groups: plan_groups,
        estimated_io_bytes: total_io,
    })
}

pub async fn partition_health(h: &DeltaTableHandle, by: &[String]) -> Result<PartitionReport> {
    let files = list_active_files(h, None).await?;
    let mut value_sets: Vec<(String, std::collections::BTreeSet<String>)> =
        by.iter().map(|k| (k.clone(), Default::default())).collect();
    let mut empty_partitions = 0usize;

    for f in &files {
        if f.size <= 0 { empty_partitions += 1; }
        for (k, set) in value_sets.iter_mut() {
            let v = f.partition_values.get(k).and_then(|o| o.clone()).unwrap_or_else(|| "__UNKNOWN__".to_string());
            set.insert(v);
        }
    }
    let cardinality = value_sets
        .into_iter()
        .map(|(k, set)| PartitionCardinality { key: k, distinct: set.len() })
        .collect();
    Ok(PartitionReport { by: by.to_vec(), cardinality, empty_partitions, total_files: files.len() })
}

pub async fn diff_versions(h: &DeltaTableHandle, from: i64, to: i64) -> Result<DiffReport> {
    if to < from { return Err(anyhow!("to must be >= from")); }
    let files_from = list_active_files(h, Some(from)).await?;
    let files_to = list_active_files(h, Some(to)).await?;

    use std::collections::HashSet;
    let mut map_from: HashMap<String, i64> = HashMap::new();
    for f in files_from { map_from.insert(f.path.clone(), f.size as i64); }
    let mut map_to: HashMap<String, i64> = HashMap::new();
    for f in files_to { map_to.insert(f.path.clone(), f.size as i64); }

    let set_from: HashSet<String> = map_from.keys().cloned().collect();
    let set_to: HashSet<String> = map_to.keys().cloned().collect();
    let added: HashSet<_> = set_to.difference(&set_from).collect();
    let removed: HashSet<_> = set_from.difference(&set_to).collect();

    let files_added = added.len();
    let files_removed = removed.len();
    let bytes_added: i64 = added.into_iter().map(|p| map_to.get(p).copied().unwrap_or(0)).sum();
    let bytes_removed: i64 = removed.into_iter().map(|p| map_from.get(p).copied().unwrap_or(0)).sum();

    Ok(DiffReport { from, to, files_added, files_removed, bytes_added, bytes_removed })
}

pub async fn generate_manifest(h: &DeltaTableHandle, version: i64, _format: ManifestFormat) -> Result<Manifest> {
    let files = list_active_files(h, Some(version)).await?;
    let entries = files.into_iter().map(|f| ManifestEntry { path: f.path, size: f.size }).collect();
    Ok(Manifest { version, files: entries })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ManifestFormat { Trino, Hive, Presto, FileList }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VacuumReport {
    pub referenced_files: usize,
    pub existing_files: usize,
    pub orphans: usize,
    pub safe: bool,
}

pub async fn vacuum_dry_run(h: &DeltaTableHandle, _retention_days: i64) -> Result<VacuumReport> {
    let parsed = parse_uri(&h.uri)?;
    let store = make_object_store(&h.uri, &StorageOptions::default()).await?;
    let active = list_active_files(h, None).await?;
    let prefix = object_path_from_url(&parsed.url);
    let listing = storage::list_recursively(store, &prefix).await?;

    use std::collections::HashSet;
    let mut referenced: HashSet<String> = HashSet::new();
    for f in active { referenced.insert(f.path); }
    let root_str = prefix.as_ref();
    let mut norm_existing: HashSet<String> = HashSet::new();
    for m in listing {
        let full = m.location.as_ref();
        let rel = full.strip_prefix(root_str).unwrap_or(full).trim_start_matches('/') .to_string();
        if rel.starts_with("_delta_log/") || rel.is_empty() { continue; }
        norm_existing.insert(rel);
    }
    let orphans: usize = norm_existing.difference(&referenced).count();
    let safe = orphans == 0;
    Ok(VacuumReport { referenced_files: referenced.len(), existing_files: norm_existing.len(), orphans, safe })
}


