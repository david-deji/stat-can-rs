use polars::prelude::*;
use crate::StatCanError;

pub struct StatCanDataFrame {
    df: DataFrame,
}

impl StatCanDataFrame {
    pub fn new(df: DataFrame) -> Self {
        Self { df }
    }

    pub fn into_polars(self) -> DataFrame {
        self.df
    }

    pub fn as_polars(&self) -> &DataFrame {
        &self.df
    }

    /// Filter by Geography (GEO column)
    /// Supports literal matching or partial matching if strict is false (but we'll stick to simple contains for now)
    pub fn filter_geo(self, pattern: &str) -> Result<Self, StatCanError> {
        let df = self.df.lazy()
            .filter(col("GEO").str().contains_literal(lit(pattern)))
            .collect()?;
        Ok(Self { df })
    }

    /// Filter by Reference Date (REF_DATE column)
    /// Assumes REF_DATE is in "YYYY-MM" or "YYYY-MM-DD" format
    pub fn filter_date_range(self, start_year: i32, end_year: i32) -> Result<Self, StatCanError> {
        // We need to parse REF_DATE first if it's not already a date
        // StatCan usually gives "YYYY-MM" strings.
        // We append "-01" to make it a valid date for parsing if it's just YYYY-MM
        let df = self.df.lazy()
            .with_column(
                (col("REF_DATE") + lit("-01"))
                    .str().strptime(DataType::Date, StrptimeOptions {
                        format: Some("%Y-%m-%d".into()),
                        strict: false, 
                        exact: false, 
                        ..Default::default()
                    }, lit("raise"))
                    .alias("parsed_date")
            )
            .filter(col("parsed_date").dt().year().gt_eq(lit(start_year)))
            .filter(col("parsed_date").dt().year().lt_eq(lit(end_year)))
            .collect()?;
            
        Ok(Self { df })
    }
    
    /// Filter by a specific column value
    pub fn filter_column(self, col_name: &str, value: &str) -> Result<Self, StatCanError> {
        let df = self.df.lazy()
            .filter(col(col_name).eq(lit(value)))
            .collect()?;
        Ok(Self { df })
    }

    /// Inspect unique values of a column (useful for debugging)
    pub fn inspect_column(&self, col_name: &str) -> Result<(), StatCanError> {
        let unique = self.df.column(col_name).map_err(StatCanError::from)?.unique()?;
        println!("Unique values for '{}':", col_name);
        println!("{:?}", unique.head(Some(20)));
        Ok(())
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
        ).unwrap()
    }

    #[test]
    fn test_filter_geo() {
        let df = create_mock_df();
        let wrapper = StatCanDataFrame::new(df);
        let filtered = wrapper.filter_geo("Ontario").unwrap();
        let res = filtered.as_polars();
        
        assert_eq!(res.height(), 1);
        assert_eq!(res.column("GEO").unwrap().get(0).unwrap(), AnyValue::String("Ontario"));
    }

    #[test]
    fn test_filter_date_range() {
        let df = create_mock_df();
        let wrapper = StatCanDataFrame::new(df);
        // 2021 to 2022
        let filtered = wrapper.filter_date_range(2021, 2022).unwrap();
        let res = filtered.as_polars();
        
        assert_eq!(res.height(), 2); // 2021-06 and 2022-12
        
        let dates: Vec<String> = res.column("REF_DATE").unwrap().str().unwrap().into_iter().flatten().map(|s| s.to_string()).collect();
        assert!(dates.contains(&"2021-06".to_string()));
        assert!(dates.contains(&"2022-12".to_string()));
    }

    #[test]
    fn test_filter_column() {
        let df = create_mock_df();
        let wrapper = StatCanDataFrame::new(df);
        let filtered = wrapper.filter_column("Category", "A").unwrap();
        let res = filtered.as_polars();
        
        assert_eq!(res.height(), 2); // Canada (A) and Alberta (A)
    }
}
