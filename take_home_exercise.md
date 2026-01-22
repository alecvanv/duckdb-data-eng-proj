# Bees & Bears - Data Engineer Take-Home Exercise

## Overview
At Bees & Bears, we finance green energy installations (solar panels, heat pumps, battery storage) by partnering with installers. Customers apply for financing, we underwrite the loans, and our Risk team monitors portfolio performance.

You'll work with realistic (but simplified) data from our loan origination process. Your task is to build an ETL pipeline that transforms raw application data into clean, analysis-ready datasets for our Risk and Finance teams.

**Time commitment:** We expect this to take 3-4 hours. Please don't spend more than 5 hours total.

---

## The Challenge

### Context
Our loan applications arrive from partner installers via API and are stored in our application database. We also receive daily updates from our Loan Management System (LMS) about loan performance. The Risk team currently exports data manually into spreadsheets, which is error-prone and time-consuming.

Your job: Build a data pipeline that automates this process.

### What We're Providing

1. **`applications.csv`** - Raw loan application data from our system
2. **`lms_updates.csv`** - Daily loan performance data from our LMS

### Your Deliverables

1. **Python ETL Pipeline** (`pipeline.py` or similar)
   - Ingest and validate the raw data
   - Transform it according to business rules
   - Output clean datasets ready for analysis

2. **SQL Queries** (`queries.sql`)
   - Write 3-5 SQL queries that the Risk team would run on your cleaned data
   - Include at least one query with aggregation/window functions

3. **Documentation** (`README.md`)
   - Your design decisions and trade-offs
   - Data quality checks you implemented
   - How you'd deploy this in production (brief - 2-3 paragraphs)
   - What you'd improve with more time

---

## Business Rules & Requirements

### Data Quality Requirements

1. **Application Data Validations:**
   - `application_id` must be unique and non-null
   - `loan_amount_eur` must be positive
   - `credit_score` should be between 300-850 (flag if missing or out of range)
   - `postal_code` should be valid German format (5 digits)
   - `installation_type` must be one of: solar_pv, solar_battery, heat_pump
   - `system_size_kwp` should be positive (if applicable for installation type)

2. **LMS Data Validations:**
   - `loan_id` must be unique
   - `application_id` must match an approved application
   - `current_balance_eur` must be ≤ original loan amount
   - `disbursement_date` must be after application_date

3. **Data Transformations:**
   - Standardize email domains (lowercase)
   - Create `risk_category` field based on credit_score:
     - Excellent: 750+
     - Good: 700-749
     - Fair: 650-699
     - Poor: <650
     - Unknown: missing score
   - Calculate `loan_to_income_ratio` = loan_amount / annual_income
   - Create `delinquency_bucket` from days_past_due:
     - Current: 0 days
     - Late: 1-30 days
     - Delinquent: 31-90 days
     - Default: 90+ days

### Output Requirements

**Table 1: `cleaned_applications`**
Should include all original fields plus:
- `risk_category`
- `loan_to_income_ratio`
- `data_quality_flags` (JSON/dict of any validation issues)
- `processed_at` (timestamp)

**Table 2: `loan_portfolio`**
Join applications + LMS data to create comprehensive loan view:
- All application fields
- All LMS fields
- `delinquency_bucket`
- `months_since_disbursement`
- `estimated_remaining_balance` (current balance if available, else original amount)

**Table 3: `data_quality_report`**
Summary of issues found:
- Count of records processed
- Count of validation failures by type
- List of problematic application_ids

---

## SQL Query Requirements

Write queries that would help the Risk team answer:

1. **Portfolio Overview:** Monthly cohort analysis showing total loan volume, average loan size, and approval rate by installation type

2. **Risk Monitoring:** Identify all loans with credit_score < 680 AND loan_to_income_ratio > 0.35

3. **Delinquency Analysis:** Calculate delinquency rate by installer partner and risk category

4. **Performance Tracking:** Show 30/60/90 day delinquency rates for each monthly cohort

5. **(Your choice):** One additional query you think would be valuable for portfolio management

---

## Technical Considerations

- Use Python 3.9+ with pandas (other libraries as needed)
- Write clean, readable code with comments where helpful
- Handle errors gracefully (don't just crash on bad data)
- Think about how this would run daily in production
- Consider memory efficiency (what if files were 100x larger?)

---

## Evaluation Criteria

We're looking for:
- **Correctness:** Does it work? Does it handle edge cases?
- **Data Quality Focus:** Do you proactively identify and handle issues?
- **Code Quality:** Is it readable, maintainable, and well-structured?
- **SQL Skills:** Can you write efficient, correct queries for business questions?
- **Communication:** Can you explain your technical decisions clearly?
- **Pragmatism:** Do you balance "perfect" with "good enough for now"?

---

## Submission

Please submit:
1. All code files (Python, SQL)
2. README.md with your documentation
3. Output files (CSVs or similar) showing your pipeline results
4. Any additional files you created

Email as a ZIP file or share a GitHub repository link.

--- 
This task is designed to be completed in approximately 3-4 hours. We don't expect it to be perfect. If you find yourself spending significantly more time, try to simplify your approach and document any assumptions or shortcuts you have taken in the README.

There are areas in which this challenge is vague, this is intentional. We expect you to be able to make decisions yourself about what to do. There is no singular correct approach to any problem.

If you do find yourself stuck, please reach out to us and we may be able to offer advice or assistance.

Good luck!


