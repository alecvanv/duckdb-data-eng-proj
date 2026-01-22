# Data Pipeline Take-Home Exercise

## Overview
At Bees & Bears, we finance green energy installations (solar panels, heat pumps, battery storage) by partnering with installers. Customers apply for financing, we underwrite the loans, and our Risk team monitors portfolio performance.

You'll work with realistic (but simplified) data from our loan origination process. Your task is to build an ETL pipeline that transforms raw application data into clean, analysis-ready datasets for our Risk and Finance teams.
---

## Design Decisions and Trade-offs

- **Why DuckDB?**

DuckDB was chosen because this is a self-contained exercise that doesnt need any external infrastructure. It allows the entire pipeline to run locally in a single Python process while still expressing most of the logic in SQL, which also aligns well with the second part of the exercise that focuses on writing/developing SQL queries.

Compared to alternatives, DuckDB is a good middle ground: more structured and reproducible than a pandas-only approach, but without the overhead of setting up a full database or a system like Spark. It also handles CSV ingestion well.

For larger datasets or multi-user production systems, a traditional warehouse would be more appropriate, but for this scope DuckDB keeps the solution simple and easy to review.


- **Quarantining malformed rows**  
  Some application rows contained more delimiters than defined in the header (typically due to unescaped commas). These rows are quarantined instead of auto-corrected to avoid silent data corruption.

- **Postcode as VARCHAR**
    `postcode` was ingested as a VARCHAR as to not lose leading 0's

- **Cleaned tables contain all records**  
  `cleaned_applications` and `lms_cleaned`(not included in the final export, used as a staging table) standardize types and add data-quality flags but do not drop rows with issues. This keeps the pipeline auditable and allows downstream filtering via flags rather than anything hardcoded.

- **System size validation by installation type**  
  `system_size_kwp` is enforced as required and positive for solar installations, I also flagged any non null records where installation type was a heat pump as this was unexpected.

- **Credit score categorisation**  
  Risk categories are only applied to valid credit scores. Out-of-range values are explicitly marked as invalid instead of being mapped to low-quality risk buckets.

- **Join strategy**  
  I Made the assumption to join LMS data to application data even though not all applications seen in LMS data exist in the application data. this assumption was made due to the wording of the instructions and the requirements of the sql part of the test. I can see many arguements for an inner join or joining applications to lms data as lms data is the record of movement of funds. for the sake of time i kept going with my initial assumption even if it may not have been the intended solution.

- **No automatic deduplication**  
  Duplicate application or loan IDs are flagged rather than removed, as these indicate genuine data quality issues and not pure duplicates.

?? given more time could split each query into seperately maintainable files ??  

---

## Data Quality Checks Implemented

**Applications**
- Null or duplicate `application_id`
- Negative loan amount
- Missing or out-of-bounds credit score
- Invalid postal codes
- Invalid installation type
- Invalid or negative system size values

**LMS**
- Null or duplicate `loan_id`
- Invalid or duplicate `application_id`
- Negative balances or days past due
- Inconsistent payment and disbursement dates

A `data_quality_report` table summarizes record counts, validation failures by type, quarantined records, and a list of affected application IDs.

---

## Production Deployment (Brief)
In production, this pipeline would run as a scheduled job (e.g. Airflow or similar), ingesting data into a persistent data warehouse. Outputs would be written to a data lake or warehouse (e.g. S3 + Redshift or Snowflake), with monitoring on validation thresholds to surface upstream data issues early.

---

## Improvements With More Time
- Add row-loss checks and completeness checks (e.g. using packages like Great Expectations)
- Alerting when validation failures exceed defined thresholds
- Incremental ingestion and comparison against previous runs
- Support for ingesting multiple input files at scale
- Consolidate application and LMS quality flags into a single portfolio-level view
- checks

## Key Findings During Analysis

- `loan_id` is not one-to-one with `application_id`. Some application IDs are linked to multiple loan IDs, which is unexpected. Comparing original loan amounts to current outstanding balances further suggests this is a data quality issue rather than a legitimate multi-loan scenario. In some cases, the same application ID is also associated with different customer email addresses, reinforcing this conclusion.

- A loan was found to be disbursed with an `application_id` of `app_declined`. This is a serious inconsistency, as declined applications should not appear in the LMS as active loans.

- The `loan_id` column appears to have significant data quality issues. Given that it would typically be treated as a primary key in an LMS system, this is particularly concerning and suggests upstream integrity problems.

- There are several cases where the current outstanding balance is extremely high relative to the applicant’s annual income, resulting in very high loan-to-income ratios. However a full affordability or exposure check was not implemented due to time constraints.

These findings reinforce the decision to avoid automatic deduping and instead surface issues explicitly through validation flags and reporting.

