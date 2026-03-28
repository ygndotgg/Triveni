
// pub fn write_partitioned_batch(
//     schema: Arc<Schema>,
//     batch_id: usize,
//     batch: Vec<TelemetryMessage>,
//     table_root: &Path,
// ) -> Result<Vec<PathBuf>, Box<dyn Error>> {
//     let mut grouped: BTreeMap<String, Vec<TelemetryMessage>> = BTreeMap::new();
//     for msg in batch {
//         grouped.entry(msg.event_date.clone()).or_default().push(msg);
//     }

//     let mut written_paths = Vec::with_capacity(grouped.len());
//     for (event_date, rows) in grouped {
//         let record_batch = build_record_batch(schema.clone(), &rows)?;
//         let partition_dir = table_root.join(format!("event_date={event_date}"));
//         let output_path = partition_dir.join(format!("part-{batch_id:05}.parquet"));
//         write_parquet(schema.clone(), &record_batch, &output_path)?;
//         written_paths.push(output_path);
//     }

//     Ok(written_paths)
// }

// pub fn write_parquet(
//     schema: Arc<Schema>,
//     batch: &RecordBatch,
//     path: &Path,
// ) -> Result<(), Box<dyn Error>> {
//     if let Some(parent) = path.parent() {
//         fs::create_dir_all(parent)?;
//     }

//     let file = File::create(path)?;
//     let props = WriterProperties::builder()
//         .set_compression(Compression::SNAPPY)
//         .build();
//     let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
//     writer.write(batch)?;
//     writer.close()?;

//     Ok(())
// }

// pub fn print_parquet_metadata(path: &Path) -> Result<(), Box<dyn Error>> {
//     let file = File::open(path)?;
//     let parquet_reader = SerializedFileReader::new(file)?;
//     let metadata = parquet_reader.metadata();

//     println!("Parquet file: {}", path.display());
//     println!("Parquet row groups: {}", metadata.num_row_groups());
//     for i in 0..metadata.num_row_groups() {
//         let row_group = metadata.row_group(i);
//         println!("Row group {i}: {} rows", row_group.num_rows());
//         for j in 0..row_group.num_columns() {
//             let column = row_group.column(j);
//             println!(
//                 "  column {}: {}, compressed={} bytes",
//                 j,
//                 column.column_path().string(),
//                 column.compressed_size()
//             );
//         }
//     }

//     Ok(())
// }
