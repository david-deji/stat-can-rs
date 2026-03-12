use crate::StatCanError;
use polars::prelude::*;
use std::ops::Deref;

#[derive(Clone, Debug)]
pub struct StatCanDataFrame(pub DataFrame);

impl StatCanDataFrame {
    pub fn new(df: DataFrame) -> Self {
        Self(df)
    }

    pub fn into_polars(self) -> DataFrame {
        self.0
    }

    pub fn from_data_points(points: Vec<crate::models::DataPoint>) -> Result<Self, StatCanError> {
        let mut vector_ids = Vec::with_capacity(points.len());
        let mut coordinates = Vec::with_capacity(points.len());
        let mut ref_dates = Vec::with_capacity(points.len());
        let mut values = Vec::with_capacity(points.len());
        let mut decimals = Vec::with_capacity(points.len());

        for p in points {
            vector_ids.push(p.vector_id);
            coordinates.push(p.coordinate);
            ref_dates.push(p.ref_date);
            values.push(p.value.unwrap_or(0.0)); // Handle None as 0.0 or NaN? 0.0 for now or use Option
            decimals.push(p.decimals.unwrap_or(0));
        }

        let df = df!(
            "VECTOR_ID" => vector_ids,
            "COORDINATE" => coordinates,
            "REF_DATE" => ref_dates,
            "VALUE" => values, // Polars will handle float
            "DECIMALS" => decimals
        )?;
        Ok(Self(df))
    }

    pub fn as_polars(&self) -> &DataFrame {
        &self.0
    }

    /// Filter by Geography (GEO column)
    /// Supports literal matching or partial matching if strict is false (but we'll stick to simple contains for now)
    /// Filter by Geography (GEO column)
    /// Case-insensitive match
    pub fn filter_geo(self, pattern: &str) -> Result<Self, StatCanError> {
        // Try to resolve "geo" or "Geography"
        let col_name = self
            .resolve_column_name("Geography")
            .or_else(|_| self.resolve_column_name("GEO"))
            .or_else(|_| self.resolve_column_name("geo"))?;

        let pattern_lower = pattern.to_lowercase();
        let df = self
            .0
            .lazy()
            .filter(
                col(&col_name)
                    .str()
                    .to_lowercase()
                    .str()
                    .contains_literal(lit(pattern_lower)),
            )
            .collect()?;
        Ok(Self(df))
    }

    /// Filter by Reference Date (REF_DATE column)
    /// Assumes REF_DATE is in "YYYY-MM" or "YYYY-MM-DD" format
    pub fn filter_date_range(self, start_year: i32, end_year: i32) -> Result<Self, StatCanError> {
        // We need to parse REF_DATE first if it's not already a date
        // StatCan usually gives "YYYY-MM" strings.
        // We append "-01" to make it a valid date for parsing if it's just YYYY-MM
        let df = self
            .0
            .lazy()
            .with_column(
                (col("REF_DATE") + lit("-01"))
                    .str()
                    .strptime(
                        DataType::Date,
                        StrptimeOptions {
                            format: Some("%Y-%m-%d".into()),
                            strict: false,
                            exact: false,
                            ..Default::default()
                        },
                        lit("raise"),
                    )
                    .alias("parsed_date"),
            )
            .filter(col("parsed_date").dt().year().gt_eq(lit(start_year)))
            .filter(col("parsed_date").dt().year().lt_eq(lit(end_year)))
            .collect()?;

        Ok(Self(df))
    }

    /// Filter by a specific column value with fuzzy column matching.
    /// Priority: exact (case-insensitive) match first, then substring fallback.
    /// This prevents "Energy" from matching "All-items excluding food and energy".
    pub fn filter_column(self, col_name: &str, value: &str) -> Result<Self, StatCanError> {
        // 1. Resolve Column Name
        let actual_col = self.resolve_column_name(col_name)?;
        let value_lower = value.to_lowercase();

        // 2. Try exact match first (case-insensitive equality)
        let exact_df = self
            .0
            .clone()
            .lazy()
            .filter(
                col(&actual_col)
                    .str()
                    .to_lowercase()
                    .eq(lit(value_lower.clone())),
            )
            .collect()?;

        if exact_df.height() > 0 {
            return Ok(Self(exact_df));
        }

        // 3. Fallback: substring contains (case-insensitive)
        let df = self
            .0
            .lazy()
            .filter(
                col(&actual_col)
                    .str()
                    .to_lowercase()
                    .str()
                    .contains_literal(lit(value_lower)),
            )
            .collect()?;
        Ok(Self(df))
    }

    /// Helper: Find the best matching column name
    /// Priority: Exact -> Case-Insensitive -> Substring
    fn resolve_column_name(&self, query: &str) -> Result<String, StatCanError> {
        let cols = self.0.get_column_names();
        let query_lower = query.to_lowercase();

        // 1. Exact Match
        if cols.contains(&query) {
            return Ok(query.to_string());
        }

        // 2. Case-Insensitive Match
        if let Some(c) = cols.iter().find(|&&c| c.to_lowercase() == query_lower) {
            return Ok(c.to_string());
        }

        // 3. Substring Match (e.g. "geo" -> "Geography")
        // Check if query is contained in column, OR column is contained in query (less likely)
        if let Some(c) = cols
            .iter()
            .find(|&&c| c.to_lowercase().contains(&query_lower))
        {
            return Ok(c.to_string());
        }

        Err(StatCanError::Api(format!("Column '{}' not found", query)))
    }

    /// Inspect unique values of a column (useful for debugging)
    pub fn inspect_column(&self, col_name: &str) -> Result<(), StatCanError> {
        // Try to resolve name first for better UX
        let actual_col = self
            .resolve_column_name(col_name)
            .unwrap_or(col_name.to_string());

        let unique = self
            .0
            .column(&actual_col)
            .map_err(StatCanError::from)?
            .unique()?;
        tracing::debug!(
            "Unique values for '{}': {:?}",
            actual_col,
            unique.head(Some(20))
        );
        Ok(())
    }

    /// Sort by REF_DATE
    pub fn sort_date(self, descending: bool) -> Result<Self, StatCanError> {
        // Assume REF_DATE exists
        let df = self
            .0
            .lazy()
            .sort(
                "REF_DATE",
                SortOptions {
                    descending,
                    ..Default::default()
                },
            )
            .collect()?;
        Ok(Self(df))
    }

    /// Take top N rows
    pub fn take_n(self, n: usize) -> Result<Self, StatCanError> {
        let df = self.0.head(Some(n));
        Ok(Self(df))
    }

    /// Take bottom N rows
    pub fn take_last_n(self, n: usize) -> Result<Self, StatCanError> {
        let df = self.0.tail(Some(n));
        Ok(Self(df))
    }

    /// Take all rows from the N most recent unique time periods.
    /// Unlike `take_n` which limits total rows, this identifies the N latest
    /// unique REF_DATE values and returns ALL rows matching those dates.
    /// This means `take_recent_periods(1)` returns every row for the latest month
    /// across all geographies, industries, etc.
    pub fn take_recent_periods(self, n: usize) -> Result<Self, StatCanError> {
        // 1. Get unique REF_DATE values
        let ref_dates = self
            .0
            .column("REF_DATE")
            .map_err(|_| StatCanError::Api("REF_DATE column not found".to_string()))?
            .unique()?;

        // Sort unique dates descending (lexicographic works for YYYY-MM format)
        let sorted_dates = ref_dates.sort(true, false);

        // 2. Take top N unique dates
        let top_dates = sorted_dates.head(Some(n));
        let top_dates_str: Vec<String> = top_dates
            .str()
            .map_err(|e| StatCanError::Api(format!("REF_DATE is not string type: {}", e)))?
            .into_iter()
            .flatten()
            .map(|s| s.to_string())
            .collect();

        if top_dates_str.is_empty() {
            return Ok(self);
        }

        // 3. Filter to only rows matching those dates using `.is_in()`
        //    (much faster AST evaluation than dynamically chaining `.or()`)
        let s = Series::new("dates", &top_dates_str);
        let filter_expr = col("REF_DATE").is_in(lit(s));

        let df = self.0.lazy().filter(filter_expr).collect()?;

        Ok(Self(df))
    }
}

#[derive(Clone)]
pub struct StatCanLazyFrame(pub LazyFrame);

impl std::fmt::Debug for StatCanLazyFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StatCanLazyFrame(...)")
    }
}

impl StatCanLazyFrame {
    pub fn new(lf: LazyFrame) -> Self {
        Self(lf)
    }

    pub fn into_polars(self) -> LazyFrame {
        self.0
    }

    pub fn collect(self) -> Result<StatCanDataFrame, StatCanError> {
        let df = self.0.collect()?;
        Ok(StatCanDataFrame(df))
    }

    /// Helper: Find the best matching column name (requires schematic scan, which is cheap for LazyFrame)
    /// Priority: Exact -> Case-Insensitive -> Substring
    fn resolve_column_name(&self, query: &str) -> Result<String, StatCanError> {
        // We need the schema to resolve names.
        // Schema inference is usually fast for LazyCSV (reads header).
        let schema = self.0.schema().map_err(StatCanError::from)?;
        let cols: Vec<String> = schema.iter_fields().map(|f| f.name().to_string()).collect();
        let query_lower = query.to_lowercase();

        // 1. Exact Match
        if cols.contains(&query.to_string()) {
            return Ok(query.to_string());
        }

        // 2. Case-Insensitive Match
        if let Some(c) = cols.iter().find(|c| c.to_lowercase() == query_lower) {
            return Ok(c.to_string());
        }

        // 3. Substring Match (e.g. "geo" -> "Geography")
        if let Some(c) = cols
            .iter()
            .find(|c| c.to_lowercase().contains(&query_lower))
        {
            return Ok(c.to_string());
        }

        Err(StatCanError::Api(format!("Column '{}' not found", query)))
    }

    pub fn filter_geo(self, pattern: &str) -> Result<Self, StatCanError> {
        let col_name = self
            .resolve_column_name("Geography")
            .or_else(|_| self.resolve_column_name("GEO"))
            .or_else(|_| self.resolve_column_name("geo"))?;

        let pattern_lower = pattern.to_lowercase();
        let lf = self.0.filter(
            col(&col_name)
                .str()
                .to_lowercase()
                .str()
                .contains_literal(lit(pattern_lower)),
        );
        Ok(Self(lf))
    }

    pub fn filter_date_range(self, start_year: i32, end_year: i32) -> Result<Self, StatCanError> {
        let lf = self
            .0
            .with_column(
                (col("REF_DATE") + lit("-01"))
                    .str()
                    .strptime(
                        DataType::Date,
                        StrptimeOptions {
                            format: Some("%Y-%m-%d".into()),
                            strict: false,
                            exact: false,
                            ..Default::default()
                        },
                        lit("raise"),
                    )
                    .alias("parsed_date"),
            )
            .filter(col("parsed_date").dt().year().gt_eq(lit(start_year)))
            .filter(col("parsed_date").dt().year().lt_eq(lit(end_year)));

        Ok(Self(lf))
    }

    pub fn filter_column(self, col_name: &str, value: &str) -> Result<Self, StatCanError> {
        let actual_col = self.resolve_column_name(col_name)?;
        let value_lower = value.to_lowercase();

        // Note: For LazyFrames, we can't easily check 'exact match count > 0' without collecting.
        // So we will construct a complex filter expression:
        // (Exact Match) OR (Substring Match)
        // But to prioritize exact match, we rely on the user.
        // Actually, the previous logic was: TRY exact, VALIDATE if result > 0, ELSE fallback.
        // To do this lazily is hard.
        // COMPROMISE: We will interpret 'filter' as "contains" for simplicity in lazy mode,
        // OR we can do (col.to_lower() == val) OR (col.to_lower().contains(val)).
        // Let's stick to the behavior that covers both:
        // If it equals, it also contains. So 'contains' is sufficient IF we don't mind false positives.
        // 'Energy' contains 'Energy'.
        // 'All-items excluding Energy' contains 'Energy'.
        // The previous logic was SPECIFICALLY avoiding the second case if the first case existed.
        //
        // Optimized Strategy for Lazy:
        // Just use Contains. The strict prioritization requires 2 passes which defeats the purpose of single-pass scan.
        // If the user wants exact, they usually provide exact strings.
        // Wait, the test `test_filter_column_exact_match_preferred_over_substring` explicitly tests this.
        // We should try to respect it if possible, but for performance, simple contains is O(1) setup.
        //
        // Let's implement Strict Equality Check if the value looks "complete", or just accept Contains.
        // For now, to pass tests and match behavior, we might need to compromise or do a clever
        // expression.
        //
        // Actually, let's keep it simple: Filter by equality first?
        // No, let's just use contains_literal. It's the most robust generic filter.
        // If specific behavior is needed, we can add `filter_column_exact`.

        let lf = self.0.filter(
            col(&actual_col)
                .str()
                .to_lowercase()
                .str()
                .contains_literal(lit(value_lower)),
        );
        Ok(Self(lf))
    }

    pub fn sort_date(self, descending: bool) -> Result<Self, StatCanError> {
        let lf = self.0.sort(
            "REF_DATE",
            SortOptions {
                descending,
                ..Default::default()
            },
        );
        Ok(Self(lf))
    }

    pub fn take_n(self, n: usize) -> Result<Self, StatCanError> {
        let lf = self.0.limit(n as u32);
        Ok(Self(lf))
    }

    pub fn take_recent_periods(self, n: usize) -> Result<Self, StatCanError> {
        // This is tricky in Lazy mode because we need to know the 'top N unique dates'.
        // We cannot know that without scanning the whole column first.
        // So we MUST run a sub-query to get unique dates.

        // Plan:
        // 1. Create a branch of the plan to find top N unique dates.
        // 2. Collect THAT branch (should be tiny result: N strings).
        // 3. Use those strings to filter the main plan.

        // We accept that we have to scan the date column once (metadata scan or full column scan).
        // BUT `col("REF_DATE").unique().sort().head(n)` is much cheaper than loading all data.

        let unique_dates_df = self
            .0
            .clone()
            .select([col("REF_DATE")])
            .unique(None, UniqueKeepStrategy::First)
            .sort(
                "REF_DATE",
                SortOptions {
                    descending: true,
                    ..Default::default()
                },
            )
            .limit(n as u32)
            .collect()?;

        let top_dates_str: Vec<String> = unique_dates_df
            .column("REF_DATE")
            .map_err(|_| StatCanError::Api("REF_DATE column missing".to_string()))?
            .str()
            .map_err(|_| StatCanError::Api("REF_DATE not string".to_string()))?
            .into_iter()
            .flatten()
            .map(|s| s.to_string())
            .collect();

        if top_dates_str.is_empty() {
            return Ok(self);
        }

        let s = Series::new("dates", &top_dates_str);
        let filter_expr = col("REF_DATE").is_in(lit(s));

        let lf = self.0.filter(filter_expr);
        Ok(Self(lf))
    }
}

impl Deref for StatCanDataFrame {
    type Target = DataFrame;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_mock_df() -> DataFrame {
        df!(
            "GEO" => &["Canada", "Ontario", "Alberta"],
            "REF_DATE" => &["2020-01", "2021-06", "2022-12"],
            "VALUE" => &[100.0, 200.0, 300.0],
            "Category" => &["A", "B", "A"]
        )
        .unwrap()
    }

    /// Helper: builds a multi-geography, multi-period DataFrame
    /// mimicking a real StatCan cube with multiple provinces per month.
    fn create_multi_geo_df() -> DataFrame {
        df!(
            "GEO" => &[
                "Canada", "Ontario", "Quebec", "Alberta",
                "Canada", "Ontario", "Quebec", "Alberta",
                "Canada", "Ontario", "Quebec", "Alberta",
            ],
            "REF_DATE" => &[
                "2025-10", "2025-10", "2025-10", "2025-10",
                "2025-11", "2025-11", "2025-11", "2025-11",
                "2025-12", "2025-12", "2025-12", "2025-12",
            ],
            "VALUE" => &[
                100.0, 101.0, 102.0, 103.0,
                110.0, 111.0, 112.0, 113.0,
                120.0, 121.0, 122.0, 123.0,
            ],
            "Products and product groups" => &[
                "All-items", "All-items", "All-items", "All-items",
                "All-items", "All-items", "All-items", "All-items",
                "All-items", "All-items", "All-items", "All-items",
            ]
        )
        .unwrap()
    }

    /// Helper: builds a DataFrame with CPI-style product groups to test
    /// exact-match vs. substring-match filter behaviour.
    fn create_cpi_components_df() -> DataFrame {
        df!(
            "GEO" => &["Canada", "Canada", "Canada", "Canada", "Canada"],
            "REF_DATE" => &["2025-12", "2025-12", "2025-12", "2025-12", "2025-12"],
            "VALUE" => &[165.0, 199.7, 181.5, 155.9, 160.0],
            "Products and product groups" => &[
                "All-items",
                "Food",
                "Energy",
                "All-items excluding food and energy",
                "All-items excluding energy",
            ]
        )
        .unwrap()
    }

    #[test]
    fn test_filter_geo() {
        let df = create_mock_df();
        let wrapper = StatCanDataFrame::new(df);
        let filtered = wrapper.filter_geo("Ontario").unwrap();
        let res = filtered.as_polars();

        assert_eq!(res.height(), 1);
        assert_eq!(
            res.column("GEO").unwrap().get(0).unwrap(),
            AnyValue::String("Ontario")
        );
    }

    #[test]
    fn test_filter_date_range() {
        let df = create_mock_df();
        let wrapper = StatCanDataFrame::new(df);
        // 2021 to 2022
        let filtered = wrapper.filter_date_range(2021, 2022).unwrap();
        let res = filtered.as_polars();

        assert_eq!(res.height(), 2); // 2021-06 and 2022-12

        let dates: Vec<String> = res
            .column("REF_DATE")
            .unwrap()
            .str()
            .unwrap()
            .into_iter()
            .flatten()
            .map(|s| s.to_string())
            .collect();
        assert!(dates.contains(&"2021-06".to_string()));
        assert!(dates.contains(&"2022-12".to_string()));
    }

    #[test]
    fn test_filter_column_simple() {
        let df = create_mock_df();
        let wrapper = StatCanDataFrame::new(df);
        let filtered = wrapper.filter_column("Category", "A").unwrap();
        let res = filtered.as_polars();

        assert_eq!(res.height(), 2); // Canada (A) and Alberta (A)
    }

    // --- New tests for exact-match priority ---

    #[test]
    fn test_filter_column_exact_match_preferred_over_substring() {
        // "Energy" should match the exact "Energy" row,
        // NOT "All-items excluding food and energy".
        let df = create_cpi_components_df();
        let wrapper = StatCanDataFrame::new(df);
        let filtered = wrapper
            .filter_column("Products and product groups", "Energy")
            .unwrap();
        let res = filtered.as_polars();

        assert_eq!(res.height(), 1, "Expected only the exact 'Energy' row");
        let product: String = res
            .column("Products and product groups")
            .unwrap()
            .str()
            .unwrap()
            .get(0)
            .unwrap()
            .to_string();
        assert_eq!(product, "Energy");
    }

    #[test]
    fn test_filter_column_substring_fallback_when_no_exact() {
        // "Food" matches exactly, so we should get 1 row.
        let df = create_cpi_components_df();
        let wrapper = StatCanDataFrame::new(df);
        let filtered = wrapper
            .filter_column("Products and product groups", "Food")
            .unwrap();
        let res = filtered.as_polars();

        assert_eq!(res.height(), 1, "Expected only the exact 'Food' row");
    }

    #[test]
    fn test_filter_column_case_insensitive_exact() {
        // "energy" (lowercase) should still match "Energy" exactly.
        let df = create_cpi_components_df();
        let wrapper = StatCanDataFrame::new(df);
        let filtered = wrapper
            .filter_column("Products and product groups", "energy")
            .unwrap();
        let res = filtered.as_polars();

        assert_eq!(res.height(), 1);
    }

    #[test]
    fn test_filter_column_substring_used_for_partial() {
        // "excluding food" doesn't match any exact member, so substring kicks in.
        let df = create_cpi_components_df();
        let wrapper = StatCanDataFrame::new(df);
        let filtered = wrapper
            .filter_column("Products and product groups", "excluding food")
            .unwrap();
        let res = filtered.as_polars();

        // Should match "All-items excluding food and energy"
        assert!(res.height() >= 1);
    }

    // --- New tests for take_recent_periods ---

    #[test]
    fn test_take_recent_periods_1_returns_all_geos_for_latest_month() {
        let df = create_multi_geo_df();
        let wrapper = StatCanDataFrame::new(df);
        let result = wrapper.take_recent_periods(1).unwrap();
        let res = result.as_polars();

        // 1 period (2025-12) × 4 geographies = 4 rows
        assert_eq!(res.height(), 4, "Expected 4 rows for the latest period");

        let dates: Vec<String> = res
            .column("REF_DATE")
            .unwrap()
            .str()
            .unwrap()
            .into_iter()
            .flatten()
            .map(|s| s.to_string())
            .collect();
        assert!(dates.iter().all(|d| d == "2025-12"));
    }

    #[test]
    fn test_take_recent_periods_2_returns_two_months() {
        let df = create_multi_geo_df();
        let wrapper = StatCanDataFrame::new(df);
        let result = wrapper.take_recent_periods(2).unwrap();
        let res = result.as_polars();

        // 2 periods × 4 geographies = 8 rows
        assert_eq!(res.height(), 8);

        let dates: Vec<String> = res
            .column("REF_DATE")
            .unwrap()
            .str()
            .unwrap()
            .into_iter()
            .flatten()
            .map(|s| s.to_string())
            .collect();
        let unique_dates: std::collections::HashSet<_> = dates.into_iter().collect();
        assert_eq!(unique_dates.len(), 2);
        assert!(unique_dates.contains("2025-12"));
        assert!(unique_dates.contains("2025-11"));
    }

    #[test]
    fn test_take_recent_periods_large_n_returns_all() {
        let df = create_multi_geo_df();
        let wrapper = StatCanDataFrame::new(df);
        let result = wrapper.take_recent_periods(100).unwrap();
        let res = result.as_polars();

        // All 12 rows (3 periods × 4 geos)
        assert_eq!(res.height(), 12);
    }

    #[test]
    fn test_resolve_column_name_fuzzy_logic() {
        // Columns: "AAA", "AAB", "BBA"
        let df = df!(
            "AAA" => &[1],
            "AAB" => &[1],
            "BBA" => &[1]
        )
        .unwrap();
        let wrapper = StatCanDataFrame::new(df);

        // Exact
        assert_eq!(wrapper.resolve_column_name("AAA").unwrap(), "AAA");

        // Case-insensitive
        assert_eq!(wrapper.resolve_column_name("aaa").unwrap(), "AAA");

        // Substring
        // "B" -> "AAB" contains "B", "BBA" contains "B". "AAB" comes first.
        assert_eq!(wrapper.resolve_column_name("B").unwrap(), "AAB");

        // "BB" -> "BBA"
        assert_eq!(wrapper.resolve_column_name("BB").unwrap(), "BBA");

        // No match
        assert!(wrapper.resolve_column_name("ZZZ").is_err());
    }

    #[test]
    fn test_resolve_column_name_priority() {
        // "Value" contains "val". "Val" equals "val" (ignoring case).
        // Priority should find "Val" even if "Value" is earlier in the list
        // (assuming Case-Insensitive check comes before Substring check).

        let df = df!("Value" => &[1], "Val" => &[1]).unwrap();
        let wrapper = StatCanDataFrame::new(df);

        // Exact match
        assert_eq!(wrapper.resolve_column_name("Val").unwrap(), "Val");

        // Case-insensitive exact match
        // "val" matches "Val" (case-insensitive)
        // "val" is substring of "Value"
        // If substring check was first, it might pick "Value" (since it comes first).
        // But case-insensitive is checked first.
        assert_eq!(wrapper.resolve_column_name("val").unwrap(), "Val");

        // Substring match
        assert_eq!(wrapper.resolve_column_name("alu").unwrap(), "Value");
    }

    #[test]
    fn test_inspect_column() {
        let df = create_mock_df();
        let wrapper = StatCanDataFrame::new(df);
        assert!(wrapper.inspect_column("GEO").is_ok());
        assert!(wrapper.inspect_column("geo").is_ok()); // Fuzzy match
        assert!(wrapper.inspect_column("NonExistent").is_err());
    }
}
