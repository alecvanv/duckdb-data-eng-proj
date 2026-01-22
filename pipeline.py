import json
import logging
from pathlib import Path
from datetime import datetime, timezone

import duckdb

#----- Update these variables if needed -----------------------
ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
OUT_DIR = ROOT / "output"
DB_PATH = ROOT / "takehome.duckdb"

APPLICATIONS_CSV = DATA_DIR / "applications_expanded.csv"
LMS_CSV = DATA_DIR / "lms_updates_expanded.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

INSTALLATION_TYPES = ("solar_pv", "solar_battery", "heat_pump")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if not APPLICATIONS_CSV.exists():
        raise FileNotFoundError(f"Missing {APPLICATIONS_CSV}")
    if not LMS_CSV.exists():
        raise FileNotFoundError(f"Missing {LMS_CSV}")

    processed_at = datetime.now(timezone.utc).isoformat()

    con = duckdb.connect(str(DB_PATH))

    #---- Loading Raw Data into DuckDB ---------------------------------------
    
    logging.info("Loading raw CSVs into DuckDB...")
    con.execute("DROP TABLE IF EXISTS raw_applications;")
    con.execute("DROP TABLE IF EXISTS raw_lms;")

    con.execute(
        """
        CREATE TABLE raw_applications AS
        SELECT *
        FROM read_csv(
          ?,
          header=true,
          delim=',',
          quote='"',
          escape='"',
          strict_mode=false,
          null_padding=true,
          all_varchar=true
        );
        """,
        [str(APPLICATIONS_CSV)],
    )

    con.execute(
        """
        CREATE TABLE raw_lms AS
        SELECT *
        FROM read_csv(
          ?,
          header=true,
          delim=',',
          quote='"',
          escape='"',
          strict_mode=false,
          null_padding=true,
          all_varchar=true
        );
        """,
        [str(LMS_CSV)],
    )

    # ---- Cleaned applications -------------------------------------------------
    logging.info("Building cleaned_applications with validation flags and transformations...")
    
    con.execute("DROP TABLE IF EXISTS raw_applications_bad;")
    con.execute("DROP TABLE IF EXISTS raw_applications_good;")

    # NOTE:
    # Some rows contain the word "comma" inside fields (e.g. email) in row 30, which causes column shifting during CSV parsing. These rows cannot berepaired safely without making assumptions about which field is affected.
    # To avoid silent data corruption, such rows are quarantined and reported in the data quality report rather than auto-corrected.

    con.execute("""
        CREATE TABLE raw_applications_bad AS
        SELECT *
        FROM raw_applications
        WHERE column12 IS NOT NULL AND TRIM(column12) <> '';
        """)
    
    con.execute("""
        CREATE TABLE raw_applications_good AS
        SELECT
          application_id,
          customer_email,
          installer_partner_id,
          installation_type,
          system_size_kwp,
          loan_amount_eur,
          loan_term_months,
          application_date,
          credit_score,
          annual_income_eur,
          postal_code,
          status
        FROM raw_applications
        WHERE column12 IS NULL OR TRIM(column12) = '';
        """)

    # Detect duplicates
    con.execute("DROP TABLE IF EXISTS app_dupes;")
    con.execute(
        """
        CREATE TABLE app_dupes AS
        SELECT application_id, COUNT(*) AS cnt
        FROM raw_applications_good
        GROUP BY 1
        HAVING COUNT(*) > 1;
        """)

    # Build cleaned_applications
    con.execute("DROP TABLE IF EXISTS cleaned_applications;")
    con.execute(f"""
        CREATE TABLE cleaned_applications AS
        WITH typed as (
            SELECT
                application_id,
                REGEXP_REPLACE(LOWER(customer_email), '\s+', '') as customer_email,
                installer_partner_id,
                installation_type,
                TRY_CAST(system_size_kwp AS DOUBLE) as system_size_kwp,
                TRY_CAST(loan_amount_eur AS DOUBLE) as loan_amount_eur,
                TRY_CAST(loan_term_months AS INTEGER) as loan_term_months,
                TRY_CAST(application_date AS DATE) as application_date,
                TRY_CAST(credit_score AS INTEGER) as credit_score,
                TRY_CAST(annual_income_eur AS DOUBLE)  AS annual_income_eur,
                postal_code,
                LOWER(status) as status
            FROM raw_applications_good  
        ),
        base AS (
            SELECT
                *,
                (application_id IS NULL OR TRIM(application_id) = '') AS flag_application_id_null,
                (application_id IN (SELECT application_id FROM app_dupes)) AS flag_application_id_duplicate,
                (loan_amount_eur IS NULL OR loan_amount_eur <= 0) AS flag_loan_amount_non_positive,
                (credit_score IS NULL) AS flag_credit_score_missing,
                (credit_score IS NOT NULL AND (credit_score < 300 OR credit_score > 850)) AS flag_credit_score_out_of_range,
                (postal_code IS NULL OR NOT regexp_matches(CAST(postal_code AS VARCHAR), '^[0-9]{{5}}$')) AS flag_postal_code_invalid,
                (installation_type IS NULL OR installation_type NOT IN {INSTALLATION_TYPES}) AS flag_installation_type_invalid,
                (installation_type IN ('solar_pv','solar_battery') AND (system_size_kwp IS NULL OR system_size_kwp <= 0)) AS flag_system_size_invalid,
                (installation_type = 'heat_pump' AND system_size_kwp IS NOT NULL) AS flag_system_size_present_for_heat_pump
            FROM typed
        )
        SELECT
            base.*,
        
            -- Create required derived fields
            CASE
                WHEN credit_score IS NULL THEN 'Unknown'
                WHEN credit_score < 300 OR credit_score > 850 THEN 'Invalid'
                WHEN credit_score >= 750 THEN 'Excellent'
                WHEN credit_score BETWEEN 700 AND 749 THEN 'Good'
                WHEN credit_score BETWEEN 650 AND 699 THEN 'Fair'
                ELSE 'Poor'
            END AS risk_category,
        
            CASE
                WHEN annual_income_eur IS NULL OR annual_income_eur <= 0 or flag_loan_amount_non_positive THEN NULL
                ELSE loan_amount_eur / annual_income_eur
            END AS loan_to_income_ratio,
        
            -- JSON flags column as per the requirement
            to_json(
              map(
                ['application_id_null',
                 'application_id_duplicate',
                 'loan_amount_non_positive',
                 'credit_score_missing',
                 'credit_score_out_of_range',
                 'postal_code_invalid',
                 'installation_type_invalid',
                 'system_size_invalid',
                 'system_size_present_for_heat_pump'],
                [flag_application_id_null,
                 flag_application_id_duplicate,
                 flag_loan_amount_non_positive,
                 flag_credit_score_missing,
                 flag_credit_score_out_of_range,
                 flag_postal_code_invalid,
                 flag_installation_type_invalid,
                 flag_system_size_invalid,
                 flag_system_size_present_for_heat_pump]
              )
            ) AS data_quality_flags,
            
            date_trunc('second', CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Berlin') AS processed_at
            
        FROM base;
        """)

    # ---- Cleaned LMS ------------------------------------------------------
    logging.info("Building cleaned_lms with validation flags and transformations...")

    # Loan_id and application_id duplicates
    con.execute("DROP TABLE IF EXISTS lms_loan_id_dupes;")
    con.execute("""
        CREATE TABLE lms_loan_id_dupes AS
        SELECT loan_id
        FROM raw_lms
        WHERE loan_id IS NOT NULL AND TRIM(loan_id) <> ''
        GROUP BY 1
        HAVING COUNT(*) > 1;
        """)
    
    con.execute("DROP TABLE IF EXISTS lms_app_id_dupes;")
    con.execute("""
        CREATE TABLE lms_app_id_dupes AS
        SELECT application_id
        FROM raw_lms
        WHERE application_id IS NOT NULL AND TRIM(application_id) <> ''
        GROUP BY 1
        HAVING COUNT(*) > 1;
        """)

    con.execute("DROP TABLE IF EXISTS approved_applications;")
    con.execute("""
        CREATE TABLE approved_applications AS
        SELECT application_id
        FROM cleaned_applications
        WHERE status = 'approved';
        """)
    
    con.execute("DROP TABLE IF EXISTS lms_cleaned;")

    con.execute("""
        CREATE TABLE lms_cleaned AS
        WITH lms_typed AS (
            SELECT
                loan_id,
                application_id,
                TRY_CAST(disbursement_date AS DATE) AS disbursement_date,
                TRY_CAST(current_balance_eur AS DOUBLE) AS current_balance_eur,
                TRY_CAST(days_past_due AS INTEGER) AS days_past_due,
                LOWER(payment_status) AS payment_status,
                TRY_CAST(last_payment_date AS DATE) AS last_payment_date,
                TRY_CAST(next_payment_due AS DATE) AS next_payment_due
            FROM raw_lms
        ),
        base AS (
            SELECT
                *,
        
                -- Flags for nulls or incorrect formats
                (loan_id IS NULL OR TRIM(loan_id) = '') AS flag_loan_id_null,
                (application_id IS NULL OR TRIM(application_id) = '') AS flag_application_id_null,
                (application_id IS NOT NULL AND NOT regexp_matches(application_id, '^APP[0-9]+$')) AS flag_application_id_invalid_format,
                (loan_id IN (SELECT loan_id FROM lms_loan_id_dupes)) AS flag_loan_id_duplicate,
                (application_id IN (SELECT application_id FROM lms_app_id_dupes)) AS flag_application_id_duplicate,
        
                -- Business-rule flags
                (current_balance_eur IS NOT NULL AND current_balance_eur < 0) AS flag_current_balance_negative,
                (days_past_due IS NOT NULL AND days_past_due < 0) AS flag_days_past_due_negative,
        
                -- Date consistency checks
                (
                  last_payment_date IS NOT NULL
                  AND disbursement_date IS NOT NULL
                  AND last_payment_date < disbursement_date
                ) AS flag_last_payment_before_disbursement,
        
                (
                  next_payment_due IS NOT NULL
                  AND disbursement_date IS NOT NULL
                  AND next_payment_due < disbursement_date
                ) AS flag_next_due_before_disbursement,
        
                (
                  last_payment_date IS NOT NULL
                  AND next_payment_due IS NOT NULL
                  AND last_payment_date > next_payment_due
                ) AS flag_last_payment_after_next_due
        
            FROM lms_typed
        )
        SELECT
            *,
            CASE
                WHEN days_past_due IS NULL THEN NULL
                WHEN days_past_due = 0 THEN 'Current'
                WHEN days_past_due BETWEEN 1 AND 30 THEN 'Late'
                WHEN days_past_due BETWEEN 31 AND 90 THEN 'Delinquent'
                ELSE 'Default'
            END AS delinquency_bucket,
        
            -- Same JSON-of-booleans pattern as applications
            to_json(
              map(
                ['loan_id_null',
                 'application_id_null',
                 'application_id_invalid_format',
                 'loan_id_duplicate',
                 'application_id_duplicate',
                 'current_balance_negative',
                 'days_past_due_negative',
                 'last_payment_before_disbursement',
                 'next_due_before_disbursement',
                 'last_payment_after_next_due'],
                [flag_loan_id_null,
                 flag_application_id_null,
                 flag_application_id_invalid_format,
                 flag_loan_id_duplicate,
                 flag_application_id_duplicate,
                 flag_current_balance_negative,
                 flag_days_past_due_negative,
                 flag_last_payment_before_disbursement,
                 flag_next_due_before_disbursement,
                 flag_last_payment_after_next_due]
              )
            ) AS data_quality_flags,
        
            date_trunc('second', CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Berlin') AS processed_at
        
        FROM base;
        """)


    
    # ---- Loan portfolio -------------------------------------------------------
    logging.info("Building loan_portfolio (applications + LMS)...")

    con.execute("DROP TABLE IF EXISTS loan_portfolio;")
    con.execute("""
        CREATE TABLE loan_portfolio AS
        SELECT
            -- All application fields
            a.*,
        
            -- All LMS fields (renamed where collisions would occur)
            l.loan_id,
            l.application_id AS lms_application_id,
            l.disbursement_date,
            l.current_balance_eur,
            l.days_past_due,
            l.payment_status,
            l.last_payment_date,
            l.next_payment_due,
        
            -- LMS flags / QA fields
            l.flag_loan_id_null,
            l.flag_application_id_null,
            l.flag_application_id_invalid_format,
            l.flag_current_balance_negative,
            l.flag_days_past_due_negative,
            l.flag_last_payment_before_disbursement,
            l.flag_next_due_before_disbursement,
            l.flag_last_payment_after_next_due,
        
            l.data_quality_flags AS lms_data_quality_flags,
            l.processed_at AS lms_processed_at,
        
            -- Required derived fields (portfolio-level)
            CASE
                WHEN l.days_past_due IS NULL THEN NULL
                WHEN l.days_past_due = 0 THEN 'Current'
                WHEN l.days_past_due BETWEEN 1 AND 30 THEN 'Late'
                WHEN l.days_past_due BETWEEN 31 AND 90 THEN 'Delinquent'
                ELSE 'Default'
            END AS delinquency_bucket,
        
            CASE
                WHEN l.disbursement_date IS NULL THEN NULL
                ELSE date_diff('month', l.disbursement_date, CURRENT_DATE)
            END AS months_since_disbursement
        
        FROM cleaned_applications a
        LEFT JOIN lms_cleaned l
          ON a.application_id = l.application_id;
        """)

    # ---- Data quality report --------------------------------------------------
    logging.info("Building data_quality_report summary...")

    con.execute("DROP TABLE IF EXISTS data_quality_report;")
    con.execute("""
        CREATE TABLE data_quality_report AS
        WITH
        app_counts AS (
          SELECT
            COUNT(*) AS applications_processed,
            SUM(flag_application_id_null::INT) AS app_application_id_null,
            SUM(flag_application_id_duplicate::INT) AS app_application_id_duplicate,
            SUM(flag_loan_amount_non_positive::INT) AS app_loan_amount_non_positive,
            SUM(flag_credit_score_missing::INT) AS app_credit_score_missing,
            SUM(flag_credit_score_out_of_range::INT) AS app_credit_score_out_of_range,
            SUM(flag_postal_code_invalid::INT) AS app_postal_code_invalid,
            SUM(flag_installation_type_invalid::INT) AS app_installation_type_invalid,
            SUM(flag_system_size_invalid::INT) AS app_system_size_invalid,
            SUM(flag_system_size_present_for_heat_pump::INT) AS app_system_size_present_for_heat_pump
          FROM cleaned_applications
        ),
        lms_counts AS (
          SELECT
            COUNT(*) AS lms_processed,
            SUM(flag_loan_id_null::INT) AS lms_loan_id_null,
            SUM(flag_application_id_null::INT) AS lms_application_id_null,
            SUM(flag_application_id_invalid_format::INT) AS lms_application_id_invalid_format,
            SUM(flag_loan_id_duplicate::INT) AS lms_loan_id_duplicate,
            SUM(flag_application_id_duplicate::INT) AS lms_application_id_duplicate,
            SUM(flag_current_balance_negative::INT) AS lms_current_balance_negative,
            SUM(flag_days_past_due_negative::INT) AS lms_days_past_due_negative,
            SUM(flag_last_payment_before_disbursement::INT) AS lms_last_payment_before_disbursement,
            SUM(flag_next_due_before_disbursement::INT) AS lms_next_due_before_disbursement,
            SUM(flag_last_payment_after_next_due::INT) AS lms_last_payment_after_next_due
          FROM lms_cleaned
        ),
        quarantine_counts AS (
          SELECT COUNT(*) AS quarantined_applications
          FROM raw_applications_bad
        ),
        problematic_ids AS (
          SELECT DISTINCT application_id
          FROM cleaned_applications
          WHERE
            flag_application_id_null
            OR flag_application_id_duplicate
            OR flag_loan_amount_non_positive
            OR flag_credit_score_missing
            OR flag_credit_score_out_of_range
            OR flag_postal_code_invalid
            OR flag_installation_type_invalid
            OR flag_system_size_invalid
            OR flag_system_size_present_for_heat_pump
          UNION
          SELECT DISTINCT application_id
          FROM lms_cleaned
          WHERE
            flag_loan_id_null
            OR flag_application_id_null
            OR flag_application_id_invalid_format
            OR flag_loan_id_duplicate
            OR flag_application_id_duplicate
            OR flag_current_balance_negative
            OR flag_days_past_due_negative
            OR flag_last_payment_before_disbursement
            OR flag_next_due_before_disbursement
            OR flag_last_payment_after_next_due
        )
        SELECT
          -- Counts processed
          a.applications_processed,
          q.quarantined_applications,
          l.lms_processed,
        
          -- Application failures
          a.app_application_id_null,
          a.app_application_id_duplicate,
          a.app_loan_amount_non_positive,
          a.app_credit_score_missing,
          a.app_credit_score_out_of_range,
          a.app_postal_code_invalid,
          a.app_installation_type_invalid,
          a.app_system_size_invalid,
          a.app_system_size_present_for_heat_pump,
        
          -- LMS failures
          l.lms_loan_id_null,
          l.lms_application_id_null,
          l.lms_application_id_invalid_format,
          l.lms_loan_id_duplicate,
          l.lms_application_id_duplicate,
          l.lms_current_balance_negative,
          l.lms_days_past_due_negative,
          l.lms_last_payment_before_disbursement,
          l.lms_next_due_before_disbursement,
          l.lms_last_payment_after_next_due,
        
          -- List of problematic application IDs
          (SELECT array_agg(application_id ORDER BY application_id) FROM problematic_ids)
            AS problematic_application_ids,
        
          date_trunc('second', CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Berlin') AS processed_at  
        
        FROM app_counts a
        CROSS JOIN lms_counts l
        CROSS JOIN quarantine_counts q;
        """)

    # ---- Export outputs -------------------------------------------------------
    logging.info("Exporting outputs to CSV...")
    
    con.execute(
        """
        COPY cleaned_applications
        TO ?
        (HEADER, DELIMITER ',', QUOTE '"', ESCAPE '"', FORCE_QUOTE *, NULL '');
        """,
        [str(OUT_DIR / "cleaned_applications.csv")],
    )
    
    con.execute(
        """
        COPY loan_portfolio
        TO ?
        (HEADER, DELIMITER ',', QUOTE '"', ESCAPE '"', FORCE_QUOTE *, NULL '');
        """,
        [str(OUT_DIR / "loan_portfolio.csv")],
    )
    
    con.execute(
        """
        COPY data_quality_report
        TO ?
        (HEADER, DELIMITER ',', QUOTE '"', ESCAPE '"', FORCE_QUOTE *, NULL '');
        """,
        [str(OUT_DIR / "data_quality_report.csv")],
    )
    
    # Basic row count log
    app_cnt = con.execute("SELECT COUNT(*) FROM cleaned_applications;").fetchone()[0]
    port_cnt = con.execute("SELECT COUNT(*) FROM loan_portfolio;").fetchone()[0]
    logging.info(
        "Done. cleaned_applications=%s | loan_portfolio=%s",
        app_cnt,
        port_cnt,
    )
    
    con.close()
    


if __name__ == "__main__":
    main()
