-- Generate Date Dimension Table

-- This dbt macro creates a date dimension table that spans from the specified start_date to the current run's start date.
    
-- Parameters:
-- start_date: The beginning date for the date dimension table (e.g., "1996-01-01").
-- end_date: The end date for the date dimension table, typically set to the current run's start date.

-- Usage:
-- Include this macro in your dbt project to create a date dimension table that can be used for time-related analytics and reporting.


{{ dbt_date.get_date_dimension(
        start_date="1996-01-01",
        end_date=run_started_at.strftime("%Y-%m-%d")
)}}
