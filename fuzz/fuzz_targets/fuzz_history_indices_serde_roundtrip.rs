pub enum FuzzHistoryIndices<V: Clone> {
    /// Active record tracking ongoing modifications.
    Latest {
        /// Starting version number for this record
        start_version_number: HistoryNumber,
        /// Range encoding structure (may be empty)
        range_encoding: FuzzOffsetBasedVersionRange,
        /// Current value (None indicates deletion)
        latest_value: Option<V>,
    },

    /// Immutable historical record. Contains:
    /// - Non-empty range encoding ensuring valid version ranges
    Previous(FuzzNonEmptyOffsetBasedVersionRange),
}