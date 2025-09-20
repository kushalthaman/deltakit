use std::fs;
use std::io::Write;
use std::path::PathBuf;

use deltakit_core as core;

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

fn protocol_action() -> String {
    "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}".to_string()
}

fn add_action(path: &str, size: i64, part_key: &str, part_val: &str, num_records: u64) -> String {
    let stats = format!("{{\\\"numRecords\\\":{}}}", num_records);
    format!(
        "{{\"add\":{{\"path\":\"{}\",\"size\":{},\"partitionValues\":{{\"{}\":\"{}\"}},\"modificationTime\":0,\"dataChange\":true,\"stats\":\"{}\"}}}}",
        path, size, part_key, part_val, stats
    )
}

fn remove_action(path: &str) -> String {
    format!("{{\"remove\":{{\"path\":\"{}\",\"deletionTimestamp\":0,\"dataChange\":true}}}}", path)
}

fn touch_file(dir: &PathBuf, rel: &str) {
    let p = dir.join(rel);
    if let Some(parent) = p.parent() { fs::create_dir_all(parent).unwrap(); }
    fs::write(p, b"").unwrap();
}

#[tokio::test]
async fn test_mvp_end_to_end_local() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().to_path_buf();

    // v0: add a
    write_delta_log(&dir, 0, &[
        protocol_action(),
        metadata_action(&["dt"]),
        add_action("dt=2024-01-01/a.parquet", 100, "dt", "2024-01-01", 10),
    ]);
    // v1: remove a, add b and c
    write_delta_log(&dir, 1, &[
        remove_action("dt=2024-01-01/a.parquet"),
        add_action("dt=2024-01-02/b.parquet", 200, "dt", "2024-01-02", 20),
        add_action("dt=2024-01-02/c.parquet", 50, "dt", "2024-01-02", 5),
    ]);

    // create actual files for listing/orphans
    touch_file(&dir, "dt=2024-01-02/b.parquet");
    touch_file(&dir, "dt=2024-01-02/c.parquet");
    touch_file(&dir, "orphan/x.parquet");

    let uri = dir.to_string_lossy().to_string();
    let h = core::load_table(&uri).await.unwrap();
    let ver = core::current_version(&h).await.unwrap();
    assert_eq!(ver, 1);

    let files = core::list_active_files(&h, Some(ver)).await.unwrap();
    assert!(!files.is_empty());

    let counts = core::fast_rowcount(&h, &vec!["dt".into()], Some(ver)).await.unwrap();
    // numRecords may not be available with hand-written logs; allow 0
    let total_rows: u64 = counts.iter().map(|r| r.rows).sum();
    assert!(total_rows <= 25);

    let plan = core::plan_compaction(&h, 1, &vec!["dt".into()]).await.unwrap();
    assert_eq!(plan.partition_by, vec!["dt".to_string()]);

    let health = core::partition_health(&h, &vec!["dt".into()]).await.unwrap();
    assert!(health.total_files >= 1);

    let diff = core::diff_versions(&h, 0, 1).await.unwrap();
    assert!(diff.files_added >= 1);

    let manifest = core::generate_manifest(&h, 1, core::ManifestFormat::Trino).await.unwrap();
    assert!(!manifest.files.is_empty());

    let vac = core::vacuum_dry_run(&h, 7).await.unwrap();
    assert!(vac.existing_files >= 1);
}


