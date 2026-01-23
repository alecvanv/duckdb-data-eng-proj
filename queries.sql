-- Optional: curated view excluding data with data quality issues (Problematic doesnt necessarily mean not valid for analysis)
WITH
  curated_portfolio AS (
    SELECT
      *
    FROM
      loan_portfolio
    WHERE
      application_id NOT IN (
        SELECT
          application_id
        FROM
          data_quality_report,
          UNNEST(problematic_application_ids) AS t(application_id)
        WHERE
          application_id IS NOT NULL
      )
  )
SELECT
  *
FROM
  curated_portfolio;


-- 1. Portfolio Overview: Monthly cohort analysis showing total loan volume, average loan size, and approval rate by installation type
WITH
  base AS (
    SELECT
      date_trunc('month', application_date) AS cohort_month,
      installation_type,
      LOWER(status) AS status,
      loan_amount_eur
    FROM
      loan_portfolio
    WHERE
      application_date IS NOT NULL
  )
SELECT
  cohort_month,
  installation_type,
  COUNT(*) AS total_applications,
  CAST(
    SUM(
      CASE
        WHEN status = 'approved' THEN 1
        ELSE 0
      END
    ) AS INTEGER
  ) AS approved_applications,
  ROUND(
    1.0 * SUM(
      CASE
        WHEN status = 'approved' THEN 1
        ELSE 0
      END
    ) / NULLIF(COUNT(*), 0),
    4
  ) AS approval_rate,
  ROUND(
    SUM(
      CASE
        WHEN status = 'approved' THEN loan_amount_eur
        ELSE 0
      END
    ),
    2
  ) AS total_approved_loan_volume,
  ROUND(
    AVG(
      CASE
        WHEN status = 'approved' THEN loan_amount_eur
      END
    ),
    2
  ) AS avg_approved_loan_size
FROM
  base
GROUP BY
  cohort_month,
  installation_type
ORDER BY
  cohort_month,
  installation_type;


-- 2. Risk Monitoring: Identify all loans with credit_score < 680 AND loan_to_income_ratio > 0.35
SELECT
  loan_id,
  application_id,
  installer_partner_id,
  installation_type,
  credit_score,
  current_balance_eur,
  loan_amount_eur,
  annual_income_eur,
  loan_to_income_ratio,
  application_date,
  disbursement_date,
  delinquency_bucket,
  days_past_due,
  months_since_disbursement,
  "status"
FROM
  loan_portfolio
WHERE
  not(flag_credit_score_out_of_range)
  AND not(flag_credit_score_missing)
  AND not(flag_loan_id_null)
  AND loan_to_income_ratio IS NOT NULL
  AND credit_score < 680
  AND loan_to_income_ratio > 0.35
ORDER BY
  disbursement_date desc;


--3. Delinquency Analysis: Calculate delinquency rate by installer partner and risk category
WITH
  disbursed_loans AS (
    SELECT
      installer_partner_id,
      risk_category,
      days_past_due
    FROM
      loan_portfolio
    WHERE
      not(flag_loan_id_null)
  )
SELECT
  installer_partner_id,
  COUNT(*) AS total_loans,
  -- assuming definition of deliquency to be 31+ to allign with delinquency_bucket definition in the exercise description.
  SUM(
    CASE
      WHEN days_past_due > 30 THEN 1
      ELSE 0
    END
  ) AS delinquent_loans,
  ROUND(
    1.0 * SUM(
      CASE
        WHEN days_past_due > 30 THEN 1
        ELSE 0
      END
    ) / NULLIF(COUNT(*), 0),
    4
  ) AS delinquency_rate
FROM
  disbursed_loans
GROUP BY
  installer_partner_id
ORDER BY
  delinquency_rate desc,
  total_loans desc;


-- 4. Performance Tracking: Show 30/60/90 day delinquency rates for each monthly cohort
WITH
  disbursed_loans AS (
    SELECT
      date_trunc('month', disbursement_date) AS cohort_month,
      days_past_due
    FROM
      loan_portfolio
    WHERE
      disbursement_date IS NOT NULL
      AND not(flag_loan_id_null)
  )
SELECT
  cohort_month,
  COUNT(*) AS total_loans,
  ROUND(
    1.0 * SUM(
      CASE
        WHEN days_past_due >= 30 THEN 1
        ELSE 0
      END
    ) / NULLIF(COUNT(*), 0),
    4
  ) AS dpd_30_rate,
  ROUND(
    1.0 * SUM(
      CASE
        WHEN days_past_due >= 60 THEN 1
        ELSE 0
      END
    ) / NULLIF(COUNT(*), 0),
    4
  ) AS dpd_60_rate,
  ROUND(
    1.0 * SUM(
      CASE
        WHEN days_past_due >= 90 THEN 1
        ELSE 0
      END
    ) / NULLIF(COUNT(*), 0),
    4
  ) AS dpd_90_rate
FROM
  disbursed_loans
GROUP BY
  cohort_month
ORDER BY
  cohort_month desc;


--5. (Your choice): A query to calculate each installation type’s share of monthly approved loan volume (window-function)
WITH
  monthly_volume AS (
    SELECT
      date_trunc('month', application_date) AS cohort_month,
      installation_type,
      ROUND(
        SUM(
          CASE
            WHEN LOWER(status) = 'approved' THEN loan_amount_eur
            ELSE 0
          END
        ),
        2
      ) AS approved_loan_volume
    FROM
      loan_portfolio
    WHERE
      application_date IS NOT NULL
      AND not(flag_installation_type_invalid)
    GROUP BY
      cohort_month,
      installation_type
  )
SELECT
  cohort_month,
  installation_type,
  approved_loan_volume,
  ROUND(
    approved_loan_volume / NULLIF(
      SUM(approved_loan_volume) OVER (PARTITION BY cohort_month),
      0
    ),
    4
  ) AS monthly_volume_share
FROM
  monthly_volume
ORDER BY
  cohort_month,
  installation_type;