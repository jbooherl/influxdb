//! Logic for filtering a set of Parquet files to the desired set to be used for an optimal
//! compaction operation.

use crate::{
    compact::PartitionCompactionCandidateWithInfo, parquet_file::CompactorParquetFile,
    parquet_file_lookup::ParquetFilesForCompaction,
};
use metric::{Attributes, Metric, U64Gauge, U64Histogram};
use observability_deps::tracing::*;
use std::sync::Arc;

/// Files and the budget in bytes neeeded to compact them
#[derive(Debug)]
pub(crate) struct FilteredFiles {
    /// Files with computed budget and will be compacted
    pub files: Vec<CompactorParquetFile>,
    /// Bugdet needed to compact the files
    /// If this value is 0 and the files are empty, nothing to compact.
    /// If the value is 0 and the files are not empty, there is error during estimating the budget.
    /// If the value is not 0 but the files are empty, the budget is greater than the allowed one.
    budget_bytes: u64,
    /// Partition of the files
    pub partition: Arc<PartitionCompactionCandidateWithInfo>,
}

#[derive(Debug, PartialEq)]
pub(crate) enum FilterResult {
    NothingToCompact,
    ErrorEstimatingBudget,
    OverBudget,
    Proceeed,
}

impl FilteredFiles {
    pub fn filter_result(&self) -> FilterResult {
        if self.files.is_empty() && self.budget_bytes == 0 {
            FilterResult::NothingToCompact
        } else if !self.files.is_empty() && self.budget_bytes == 0 {
            FilterResult::ErrorEstimatingBudget
        } else if self.files.is_empty() && self.budget_bytes != 0 {
            FilterResult::OverBudget
        } else {
            FilterResult::Proceeed
        }
    }

    pub fn budget_bytes(&self) -> u64 {
        self.budget_bytes
    }
}

/// Given a list of level 0 files sorted by max sequence number and a list of level 1 files for
/// a partition, select a subset set of files that:
///
/// - Has a subset of the level 0 files selected, from the start of the sorted level 0 list
/// - Has a total size less than `max_bytes`
/// - Has only level 1 files that overlap in time with the level 0 files
///
/// The returned files will be ordered with the level 1 files first, then the level 0 files ordered
/// in ascending order by their max sequence number.
pub(crate) fn filter_parquet_files(
    // partition of the parquet files
    partition: Arc<PartitionCompactionCandidateWithInfo>,
    // Level 0 files sorted by max sequence number and level 1 files in arbitrary order for one
    // partition
    parquet_files_for_compaction: ParquetFilesForCompaction,
    // Stop considering level 0 files when the total size of all files selected for compaction so
    // far exceeds this value
    max_bytes: u64,
    // Gauge for the number of Parquet file candidates
    parquet_file_candidate_gauge: &Metric<U64Gauge>,
    // Histogram for the number of bytes of Parquet file candidates
    parquet_file_candidate_bytes: &Metric<U64Histogram>,
) -> FilteredFiles {
    let (files, budget_bytes) = filter_parquet_files_inner(
        parquet_files_for_compaction,
        max_bytes,
        parquet_file_candidate_gauge,
        parquet_file_candidate_bytes,
    );

    FilteredFiles {
        files,
        budget_bytes,
        partition,
    }
}

fn filter_parquet_files_inner(
    // Level 0 files sorted by max sequence number and level 1 files in arbitrary order for one
    // partition
    parquet_files_for_compaction: ParquetFilesForCompaction,
    // Stop considering level 0 files when the total size of all files selected for compaction so
    // far exceeds this value
    max_bytes: u64,
    // Gauge for the number of Parquet file candidates
    parquet_file_candidate_gauge: &Metric<U64Gauge>,
    // Histogram for the number of bytes of Parquet file candidates
    parquet_file_candidate_bytes: &Metric<U64Histogram>,
) -> (Vec<CompactorParquetFile>, u64) {
    let ParquetFilesForCompaction {
        level_0,
        level_1: mut remaining_level_1,
        .. // Ignore other levels
    } = parquet_files_for_compaction;

    if level_0.is_empty() {
        info!("No level 0 files to consider for compaction");
        return (vec![], 0);
    }

    // Guaranteed to exist because of the empty check and early return above. Also assuming all
    // files are for the same partition.
    let partition_id = level_0[0].partition_id();

    let num_level_0_considering = level_0.len();
    let num_level_1_considering = remaining_level_1.len();

    // This will start by holding the level 1 files that are found to overlap an included level 0
    // file. At the end of this function, the level 0 files are added to the end so they are sorted
    // last.
    let mut files_to_return = Vec::with_capacity(level_0.len() + remaining_level_1.len());
    // Estimated memory bytes needed to compact returned L1 files
    let mut l1_estimated_budget = Vec::with_capacity(level_0.len() + remaining_level_1.len());
    // Keep track of level 0 files to include in this compaction operation; maintain assumed
    // ordering by max sequence number.
    let mut level_0_to_return = Vec::with_capacity(level_0.len());
    // Estimated memory bytes needed to compact returned L0 files
    let mut l0_estimated_budget = Vec::with_capacity(level_0.len());

    // Memory needed to compact the returned files
    let mut total_estimated_budget = 0;
    for level_0_file in level_0 {
        // Estimate memory needed for this L0 file
        let l0_estimated_file_bytes = level_0_file.estimated_arrow_bytes();

        // Note: even though we can stop here if the l0_estimated_file_bytes is larger than the
        // given budget,we still continue estimated the memory needed for its overlapped L1 to
        // return the total memory needed to compact this L0 with all of its overlapped L1s

        // Find all level 1 files that overlap with this level 0 file.
        let (overlaps, non_overlaps): (Vec<_>, Vec<_>) = remaining_level_1
            .into_iter()
            .partition(|level_1_file| overlaps_in_time(level_1_file, &level_0_file));

        // Estimate memory needed for each of L1
        let current_l1_estimated_file_bytes: Vec<_> = overlaps
            .iter()
            .map(|file| file.estimated_arrow_bytes())
            .collect();
        let estimated_file_bytes =
            l0_estimated_file_bytes + current_l1_estimated_file_bytes.iter().sum::<u64>();

        // Over budget
        if total_estimated_budget + estimated_file_bytes > max_bytes {
            if total_estimated_budget == 0 {
                // Cannot compact this partition further with the given budget
                return (vec![], estimated_file_bytes);
            } else {
                // Only compact the ones under the given budget
                break;
            }
        } else {
            // still under budget
            total_estimated_budget += estimated_file_bytes;
            l0_estimated_budget.push(l0_estimated_file_bytes);
            l1_estimated_budget.extend(current_l1_estimated_file_bytes);

            // Move the overlapping level 1 files to `files_to_return` so they're not considered
            // again; a level 1 file overlapping with one level 0 file is enough for its inclusion.
            // This way, we also don't include level 1 files multiple times.
            files_to_return.extend(overlaps);

            // The remaining level 1 files to possibly include in future iterations are the
            // remaining ones that did not overlap with this level 0 file.
            remaining_level_1 = non_overlaps;

            // Move the level 0 file into the list of level 0 files to return
            level_0_to_return.push(level_0_file);
        }
    }

    let num_level_0_compacting = level_0_to_return.len();
    let num_level_1_compacting = files_to_return.len();

    info!(
        partition_id = partition_id.get(),
        num_level_0_considering,
        num_level_1_considering,
        num_level_0_compacting,
        num_level_1_compacting,
        "filtered Parquet files for compaction",
    );

    record_file_metrics(
        parquet_file_candidate_gauge,
        num_level_0_considering as u64,
        num_level_1_considering as u64,
        num_level_0_compacting as u64,
        num_level_1_compacting as u64,
    );

    record_byte_metrics(
        parquet_file_candidate_bytes,
        level_0_to_return
            .iter()
            .map(|pf| pf.file_size_bytes() as u64)
            .collect(),
        files_to_return
            .iter()
            .map(|pf| pf.file_size_bytes() as u64)
            .collect(),
        l0_estimated_budget,
        l1_estimated_budget,
    );

    // Return the level 1 files first, followed by the level 0 files assuming we've maintained
    // their ordering by max sequence number.
    files_to_return.extend(level_0_to_return);
    (files_to_return, total_estimated_budget)
}

fn overlaps_in_time(a: &CompactorParquetFile, b: &CompactorParquetFile) -> bool {
    (a.min_time() <= b.min_time() && a.max_time() >= b.min_time())
        || (a.min_time() > b.min_time() && a.min_time() <= b.max_time())
}

fn record_file_metrics(
    gauge: &Metric<U64Gauge>,
    num_level_0_considering: u64,
    num_level_1_considering: u64,
    num_level_0_compacting: u64,
    num_level_1_compacting: u64,
) {
    let attributes = Attributes::from(&[
        ("compaction_level", "0"),
        ("status", "selected_for_compaction"),
    ]);
    let recorder = gauge.recorder(attributes);
    recorder.set(num_level_0_compacting);

    let attributes = Attributes::from(&[
        ("compaction_level", "0"),
        ("status", "not_selected_for_compaction"),
    ]);
    let recorder = gauge.recorder(attributes);
    recorder.set(num_level_0_considering - num_level_0_compacting);

    let attributes = Attributes::from(&[
        ("compaction_level", "1"),
        ("status", "selected_for_compaction"),
    ]);
    let recorder = gauge.recorder(attributes);
    recorder.set(num_level_1_compacting);

    let attributes = Attributes::from(&[
        ("compaction_level", "1"),
        ("status", "not_selected_for_compaction"),
    ]);
    let recorder = gauge.recorder(attributes);
    recorder.set(num_level_1_considering - num_level_1_compacting);
}

fn record_byte_metrics(
    histogram: &Metric<U64Histogram>,
    level_0_sizes: Vec<u64>,
    level_1_sizes: Vec<u64>,
    level_0_estimated_compacting_budgets: Vec<u64>,
    level_1_estimated_compacting_budgets: Vec<u64>,
) {
    let attributes = Attributes::from(&[("file_size_compaction_level", "0")]);
    let recorder = histogram.recorder(attributes);
    for size in level_0_sizes {
        recorder.record(size);
    }

    let attributes = Attributes::from(&[("file_size_compaction_level", "1")]);
    let recorder = histogram.recorder(attributes);
    for size in level_1_sizes {
        recorder.record(size);
    }

    let attributes =
        Attributes::from(&[("file_estimated_compacting_budget_compaction_level", "0")]);
    let recorder = histogram.recorder(attributes);
    for size in level_0_estimated_compacting_budgets {
        recorder.record(size);
    }

    let attributes =
        Attributes::from(&[("file_estimated_compacting_budget_compaction_level", "1")]);
    let recorder = histogram.recorder(attributes);
    for size in level_1_estimated_compacting_budgets {
        recorder.record(size);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::{
        ColumnSet, CompactionLevel, NamespaceId, ParquetFile, ParquetFileId, PartitionId,
        SequenceNumber, ShardId, TableId, Timestamp,
    };
    use metric::U64HistogramOptions;
    use std::sync::Arc;
    use uuid::Uuid;

    const BUCKET_500_KB: u64 = 500 * 1024;
    const BUCKET_1_MB: u64 = 1024 * 1024;

    #[test]
    fn test_overlaps_in_time() {
        assert_overlap((1, 3), (2, 4));
        assert_overlap((1, 3), (1, 3));
        assert_overlap((1, 3), (3, 4));
        assert_overlap((1, 4), (2, 3));
        assert_overlap((1, 3), (2, 3));
        assert_overlap((1, 3), (1, 2));

        assert_no_overlap((1, 2), (3, 4));
    }

    fn assert_overlap((a_min, a_max): (i64, i64), (b_min, b_max): (i64, i64)) {
        let a = ParquetFileBuilder::level_0()
            .min_time(a_min)
            .max_time(a_max)
            .build();
        let b = ParquetFileBuilder::level_1()
            .min_time(b_min)
            .max_time(b_max)
            .build();

        assert!(
            overlaps_in_time(&a, &b),
            "Expected ({a_min}, {a_max}) to overlap with ({b_min}, {b_max}) but it didn't",
        );
        assert!(
            overlaps_in_time(&b, &a),
            "Expected ({b_min}, {b_max}) to overlap with ({a_min}, {a_max}) but it didn't",
        );
    }

    fn assert_no_overlap((a_min, a_max): (i64, i64), (b_min, b_max): (i64, i64)) {
        let a = ParquetFileBuilder::level_0()
            .min_time(a_min)
            .max_time(a_max)
            .build();
        let b = ParquetFileBuilder::level_1()
            .min_time(b_min)
            .max_time(b_max)
            .build();

        assert!(
            !overlaps_in_time(&a, &b),
            "Expected ({a_min}, {a_max}) to not overlap with ({b_min}, {b_max}) but it did",
        );
        assert!(
            !overlaps_in_time(&b, &a),
            "Expected ({b_min}, {b_max}) to not overlap with ({a_min}, {a_max}) but it did",
        );
    }

    fn metrics() -> (Metric<U64Gauge>, Metric<U64Histogram>) {
        let registry = Arc::new(metric::Registry::new());

        let parquet_file_candidate_gauge = registry.register_metric(
            "parquet_file_candidates",
            "Number of Parquet file candidates",
        );

        let parquet_file_candidate_bytes = registry.register_metric_with_options(
            "parquet_file_candidate_bytes",
            "Number of bytes of Parquet file candidates",
            || {
                U64HistogramOptions::new([
                    BUCKET_500_KB,    // 500 KB
                    BUCKET_1_MB,      // 1 MB
                    3 * 1024 * 1024,  // 3 MB
                    10 * 1024 * 1024, // 10 MB
                    30 * 1024 * 1024, // 30 MB
                    u64::MAX,         // Inf
                ])
            },
        );

        (parquet_file_candidate_gauge, parquet_file_candidate_bytes)
    }

    mod hot {
        use super::*;

        const MEMORY_BUDGET: u64 = 1024 * 1024 * 10;

        #[test]
        fn empty_in_empty_out() {
            let parquet_files_for_compaction = ParquetFilesForCompaction {
                level_0: vec![],
                level_1: vec![],
                level_2: vec![],
            };
            let (files_metric, bytes_metric) = metrics();

            let (files, budget_bytes) = filter_parquet_files_inner(
                parquet_files_for_compaction,
                MEMORY_BUDGET,
                &files_metric,
                &bytes_metric,
            );

            assert!(files.is_empty());
            assert_eq!(budget_bytes, 0);
        }

        #[test]
        fn budget_0_returns_over_budget() {
            let parquet_files_for_compaction = ParquetFilesForCompaction {
                level_0: vec![ParquetFileBuilder::level_0().id(1).build()],
                level_1: vec![],
                level_2: vec![],
            };
            let (files_metric, bytes_metric) = metrics();

            let (files, budget_bytes) = filter_parquet_files_inner(
                parquet_files_for_compaction,
                0,
                &files_metric,
                &bytes_metric,
            );

            assert!(files.is_empty());
            assert_eq!(budget_bytes, 1176);
        }

        #[test]
        fn budget_1000_returns_over_budget() {
            let parquet_files_for_compaction = ParquetFilesForCompaction {
                level_0: vec![ParquetFileBuilder::level_0().id(1).build()],
                level_1: vec![],
                level_2: vec![],
            };
            let (files_metric, bytes_metric) = metrics();

            let (files, budget_bytes) = filter_parquet_files_inner(
                parquet_files_for_compaction,
                1000,
                &files_metric,
                &bytes_metric,
            );

            assert!(files.is_empty());
            assert_eq!(budget_bytes, 1176);
        }

        #[test]
        fn large_budget_returns_one_level_0_file_and_its_level_1_overlaps() {
            let parquet_files_for_compaction = ParquetFilesForCompaction {
                level_0: vec![ParquetFileBuilder::level_0()
                    .id(1)
                    .min_time(200)
                    .max_time(300)
                    .build()],
                level_1: vec![
                    // Too early
                    ParquetFileBuilder::level_1()
                        .id(101)
                        .min_time(1)
                        .max_time(50)
                        .build(),
                    // Completely contains the level 0 times
                    ParquetFileBuilder::level_1()
                        .id(102)
                        .min_time(150)
                        .max_time(350)
                        .build(),
                    // Too late
                    ParquetFileBuilder::level_1()
                        .id(103)
                        .min_time(400)
                        .max_time(500)
                        .build(),
                ],
                level_2: vec![],
            };
            let (files_metric, bytes_metric) = metrics();

            let (files, budget_bytes) = filter_parquet_files_inner(
                parquet_files_for_compaction,
                MEMORY_BUDGET,
                &files_metric,
                &bytes_metric,
            );

            assert_eq!(files.len(), 2);
            assert_eq!(files[0].id().get(), 102);
            assert_eq!(files[1].id().get(), 1);
            assert_eq!(budget_bytes, 2 * 1176);
        }

        #[test]
        fn returns_only_overlapping_level_1_files_in_order() {
            let parquet_files_for_compaction = ParquetFilesForCompaction {
                level_0: vec![
                    // Level 0 files that overlap in time slightly.
                    ParquetFileBuilder::level_0()
                        .id(1)
                        .min_time(200)
                        .max_time(300)
                        .file_size_bytes(10)
                        .build(),
                    ParquetFileBuilder::level_0()
                        .id(2)
                        .min_time(280)
                        .max_time(310)
                        .file_size_bytes(10)
                        .build(),
                    ParquetFileBuilder::level_0()
                        .id(3)
                        .min_time(309)
                        .max_time(350)
                        .file_size_bytes(10)
                        .build(),
                ],
                // Level 1 files can be assumed not to overlap each other.
                level_1: vec![
                    // Does not overlap any level 0, times are too early
                    ParquetFileBuilder::level_1()
                        .id(101)
                        .min_time(1)
                        .max_time(50)
                        .file_size_bytes(10)
                        .build(),
                    // Overlaps file 1
                    ParquetFileBuilder::level_1()
                        .id(102)
                        .min_time(199)
                        .max_time(201)
                        .file_size_bytes(10)
                        .build(),
                    // Overlaps files 1 and 2
                    ParquetFileBuilder::level_1()
                        .id(103)
                        .min_time(290)
                        .max_time(300)
                        .file_size_bytes(10)
                        .build(),
                    // Overlaps file 2
                    ParquetFileBuilder::level_1()
                        .id(104)
                        .min_time(305)
                        .max_time(305)
                        .file_size_bytes(10)
                        .build(),
                    // Overlaps files 2 and 3
                    ParquetFileBuilder::level_1()
                        .id(105)
                        .min_time(308)
                        .max_time(311)
                        .file_size_bytes(10)
                        .build(),
                    // Overlaps file 3
                    ParquetFileBuilder::level_1()
                        .id(106)
                        .min_time(340)
                        .max_time(360)
                        .file_size_bytes(BUCKET_500_KB as i64 + 1) // exercise metrics
                        .build(),
                    // Does not overlap any level 0, times are too late
                    ParquetFileBuilder::level_1()
                        .id(107)
                        .min_time(390)
                        .max_time(399)
                        .file_size_bytes(10)
                        .build(),
                ],
                level_2: vec![],
            };

            let (files_metric, bytes_metric) = metrics();

            let (files, budget_bytes) = filter_parquet_files_inner(
                parquet_files_for_compaction.clone(),
                1176 * 3 + 5, // enough for 3 files
                &files_metric,
                &bytes_metric,
            );

            let ids: Vec<_> = files.iter().map(|f| f.id().get()).collect();
            assert_eq!(ids, [102, 103, 1]);
            assert_eq!(budget_bytes, 3 * 1176);

            assert_eq!(
                extract_file_metrics(&files_metric),
                ExtractedFileMetrics {
                    level_0_selected: 1,
                    level_0_not_selected: 2,
                    level_1_selected: 2,
                    level_1_not_selected: 5,
                }
            );

            let (files_metric, bytes_metric) = metrics();

            let (files, budget_bytes) = filter_parquet_files_inner(
                parquet_files_for_compaction,
                // Increase budget to more than 6 files; 1st two level 0 files & their overlapping
                // level 1 files get returned
                1176 * 6 + 5,
                &files_metric,
                &bytes_metric,
            );

            let ids: Vec<_> = files.iter().map(|f| f.id().get()).collect();
            assert_eq!(ids, [102, 103, 104, 105, 1, 2]);
            assert_eq!(budget_bytes, 6 * 1176);

            assert_eq!(
                extract_file_metrics(&files_metric),
                ExtractedFileMetrics {
                    level_0_selected: 2,
                    level_0_not_selected: 1,
                    level_1_selected: 4,
                    level_1_not_selected: 3,
                }
            );
        }
    }

    /// Create ParquetFile instances for testing. Only sets fields relevant to the filtering; other
    /// fields are set to arbitrary and possibly invalid values. For example, by default, all
    /// ParquetFile instances created by this function will have the same ParquetFileId, which is
    /// invalid in production but irrelevant to this module.
    #[derive(Debug)]
    struct ParquetFileBuilder {
        compaction_level: CompactionLevel,
        id: i64,
        min_time: i64,
        max_time: i64,
        file_size_bytes: i64,
    }

    impl ParquetFileBuilder {
        // Start building a level 0 file
        fn level_0() -> Self {
            Self {
                compaction_level: CompactionLevel::Initial,
                id: 1,
                min_time: 8,
                max_time: 9,
                file_size_bytes: 10,
            }
        }

        // Start building a level 1 file
        fn level_1() -> Self {
            Self {
                compaction_level: CompactionLevel::FileNonOverlapped,
                id: 1,
                min_time: 8,
                max_time: 9,
                file_size_bytes: 10,
            }
        }

        fn id(mut self, id: i64) -> Self {
            self.id = id;
            self
        }

        fn min_time(mut self, min_time: i64) -> Self {
            self.min_time = min_time;
            self
        }

        fn max_time(mut self, max_time: i64) -> Self {
            self.max_time = max_time;
            self
        }

        fn file_size_bytes(mut self, file_size_bytes: i64) -> Self {
            self.file_size_bytes = file_size_bytes;
            self
        }

        fn build(self) -> CompactorParquetFile {
            let Self {
                compaction_level,
                id,
                min_time,
                max_time,
                file_size_bytes,
            } = self;

            let f = ParquetFile {
                id: ParquetFileId::new(id),
                shard_id: ShardId::new(2),
                namespace_id: NamespaceId::new(3),
                table_id: TableId::new(4),
                partition_id: PartitionId::new(5),
                object_store_id: Uuid::new_v4(),
                max_sequence_number: SequenceNumber::new(7),
                min_time: Timestamp::new(min_time),
                max_time: Timestamp::new(max_time),
                to_delete: None,
                file_size_bytes,
                row_count: 11,
                compaction_level,
                created_at: Timestamp::new(12),
                column_set: ColumnSet::new(std::iter::empty()),
            };
            // Estimated arrow bytes for one file with a tag, a time and 11 rows = 1176
            CompactorParquetFile::new(f, 1176)
        }
    }

    #[derive(Debug, PartialEq)]
    struct ExtractedFileMetrics {
        level_0_selected: u64,
        level_0_not_selected: u64,
        level_1_selected: u64,
        level_1_not_selected: u64,
    }

    fn extract_file_metrics(metric: &Metric<U64Gauge>) -> ExtractedFileMetrics {
        let level_0_selected = metric
            .get_observer(&Attributes::from(&[
                ("compaction_level", "0"),
                ("status", "selected_for_compaction"),
            ]))
            .unwrap()
            .fetch();

        let level_0_not_selected = metric
            .get_observer(&Attributes::from(&[
                ("compaction_level", "0"),
                ("status", "not_selected_for_compaction"),
            ]))
            .unwrap()
            .fetch();

        let level_1_selected = metric
            .get_observer(&Attributes::from(&[
                ("compaction_level", "1"),
                ("status", "selected_for_compaction"),
            ]))
            .unwrap()
            .fetch();

        let level_1_not_selected = metric
            .get_observer(&Attributes::from(&[
                ("compaction_level", "1"),
                ("status", "not_selected_for_compaction"),
            ]))
            .unwrap()
            .fetch();

        ExtractedFileMetrics {
            level_0_selected,
            level_0_not_selected,
            level_1_selected,
            level_1_not_selected,
        }
    }
}
