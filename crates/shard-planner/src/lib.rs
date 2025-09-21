use anyhow::Result;
use blake3::Hasher;
use deltakit_core as core;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BalanceMode { Bytes, Rows }

impl Default for BalanceMode { fn default() -> Self { BalanceMode::Bytes } }

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ShardOptions {
    pub by: Vec<String>,
    pub sticky_by: Vec<String>,
    pub max_files_per_shard: Option<usize>,
    pub balance: BalanceMode,
    pub row_group_aware: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardFile {
    pub path: String,
    pub bytes: i64,
    pub approx_rows: u64,
    pub partition: BTreeMap<String, Option<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Shard {
    pub id: u32,
    pub bytes: i64,
    pub rows: u64,
    pub files: Vec<ShardFile>,
}

fn stable_hash(parts: &[(String, String)]) -> u64 {
    let mut h = Hasher::new();
    for (k, v) in parts {
        h.update(k.as_bytes());
        h.update(b"=");
        h.update(v.as_bytes());
        h.update(b";");
    }
    let x = h.finalize();
    u64::from_le_bytes(x.as_bytes()[0..8].try_into().unwrap())
}

pub async fn plan_shards(
    h: &core::DeltaTableHandle,
    version: i64,
    shards: u32,
    opts: ShardOptions,
) -> Result<Vec<Shard>> {
    let files = core::list_active_files(h, Some(version)).await?;

    let mut items: Vec<ShardFile> = Vec::with_capacity(files.len());
    for f in files {
        let approx_rows = 0u64; // core fast_rowcount per-file unexposed
        items.push(ShardFile {
            path: f.path,
            bytes: f.size,
            approx_rows,
            partition: f.partition_values,
        });
    }

    // group by co-location keys (opts.by) & create buckets
    let mut groups: HashMap<Vec<(String, String)>, Vec<ShardFile>> = HashMap::new();
    for it in items.into_iter() {
        let key: Vec<(String, String)> = opts
            .by
            .iter()
            .map(|k| (k.clone(), it.partition.get(k).and_then(|o| o.clone()).unwrap_or_else(|| "__UNKNOWN__".to_string())))
            .collect();
        groups.entry(key).or_default().push(it);
    }

    // prep K shards
    let k = shards.max(1);
    let mut out: Vec<Shard> = (0..k)
        .map(|i| Shard { id: i, bytes: 0, rows: 0, files: Vec::new() })
        .collect();

    // each group assigned to shards using stable hashing over sticky_by keys then greedy balance
    for (group_key, mut files) in groups.into_iter() {
        // stable seed from sticky_by subset
        let sticky_pairs: Vec<(String, String)> = if opts.sticky_by.is_empty() {
            group_key.clone()
        } else {
            group_key
                .iter()
                .filter(|(k, _)| opts.sticky_by.iter().any(|s| s == k))
                .cloned()
                .collect()
        };
        let base_idx = (stable_hash(&sticky_pairs) % (k as u64)) as usize;

        files.sort_by_key(|f| match opts.balance { BalanceMode::Bytes => -(f.bytes as i64), BalanceMode::Rows => -(f.approx_rows as i64) });

        for f in files.into_iter() {
            let (target_idx, _) = (0..k)
                .map(|offset| ( (base_idx + offset as usize) % (k as usize) , &out[(base_idx + offset as usize) % (k as usize)]))
                .min_by_key(|(_, s)| match opts.balance { BalanceMode::Bytes => s.bytes, BalanceMode::Rows => s.rows as i64 })
                .unwrap();

            if let Some(maxf) = opts.max_files_per_shard {
                if out[target_idx].files.len() >= maxf { continue; }
            }
            out[target_idx].bytes += f.bytes.max(0);
            out[target_idx].rows += f.approx_rows;
            out[target_idx].files.push(f);
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use std::path::PathBuf;

    fn write_delta_log(dir: &PathBuf, version: u64, lines: &[String]) {
        let log_dir = dir.join("_delta_log");
        fs::create_dir_all(&log_dir).unwrap();
        let file = log_dir.join(format!("{:020}.json", version));
        let mut f = fs::File::create(file).unwrap();
        for l in lines {
            f.write_all(l.as_bytes()).unwrap();
            f.write_all(b"\n").unwrap();
        }
    }

    fn metadata_action(part_cols: &[&str]) -> String {
        let part_json = serde_json::to_string(&part_cols).unwrap();
        let schema = r#"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}"#;
        format!(
            "{{\"metaData\":{{\"id\":\"00000000-0000-0000-0000-000000000000\",\"format\":{{\"provider\":\"parquet\",\"options\":{{}}}},\"schemaString\":\"{}\",\"partitionColumns\":{},\"configuration\":{{}},\"createdTime\":0}}}}",
            schema, part_json
        )
    }

    fn protocol_action() -> String { "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}".to_string() }
    fn add_action(path: &str, size: i64, part_key: &str, part_val: &str, num_records: u64) -> String {
        let stats = format!("{{\\\"numRecords\\\":{}}}", num_records);
        format!(
            "{{\"add\":{{\"path\":\"{}\",\"size\":{},\"partitionValues\":{{\"{}\":\"{}\"}},\"modificationTime\":0,\"dataChange\":true,\"stats\":\"{}\"}}}}",
            path, size, part_key, part_val, stats
        )
    }
    fn remove_action(path: &str) -> String { format!("{{\"remove\":{{\"path\":\"{}\",\"deletionTimestamp\":0,\"dataChange\":true}}}}", path) }

    #[tokio::test]
    async fn test_shard_plan_local() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().to_path_buf();
        write_delta_log(&dir, 0, &[
            protocol_action(),
            metadata_action(&["dt"]),
            add_action("dt=2024-01-01/a.parquet", 100, "dt", "2024-01-01", 10),
        ]);
        write_delta_log(&dir, 1, &[
            remove_action("dt=2024-01-01/a.parquet"),
            add_action("dt=2024-01-02/b.parquet", 200, "dt", "2024-01-02", 20),
            add_action("dt=2024-01-02/c.parquet", 50, "dt", "2024-01-02", 5),
        ]);

        let uri = dir.to_string_lossy().to_string();
        let h = core::load_table(&uri).await.unwrap();
        let ver = core::current_version(&h).await.unwrap();
        assert_eq!(ver, 1);

        let opts = ShardOptions { by: vec!["dt".into()], sticky_by: vec!["dt".into()], max_files_per_shard: None, balance: BalanceMode::Bytes, row_group_aware: false };
        let shards = plan_shards(&h, ver, 2, opts).await.unwrap();
        assert_eq!(shards.len(), 2);
        let total_files: usize = shards.iter().map(|s| s.files.len()).sum();
        assert!(total_files >= 2);
    }
}


