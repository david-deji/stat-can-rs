use polars::prelude::*;
use crate::StatCanError;
use chrono::{Datelike, NaiveDate};

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
