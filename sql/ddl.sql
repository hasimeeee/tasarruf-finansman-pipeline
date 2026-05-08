-- ==========================================
-- STAR SCHEMA (DWH) — DWH Tabloları
-- ==========================================
-- Hafta 4 değişiklikleri:
--   - pipeline_runs: pipeline_name, finished_at sütunları eklendi
--   - quality_check_results tablosu eklendi
-- Senkronizasyon düzeltmeleri (canlı DB ile uyumlu):
--   - pipeline_runs: pipeline_run_id UUID, rows_rejected, notes eklendi
--   - quality_check_results: PK result_id → check_id, metric_value INTEGER → NUMERIC
--   - dim_branch: PK branch_key → branch_sk
--   - dim_date: day_name ve month_name kaldırıldı (day_of_week yeterli)
-- ==========================================

-- ==========================================
-- PIPELINE LOG TABLOLARI
-- ==========================================

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          SERIAL PRIMARY KEY,
    pipeline_run_id UUID,                           -- her çalışmaya ait benzersiz ID (etl_pipeline.py)
    pipeline_name   VARCHAR(100),                   -- hangi pipeline: etl / data_quality
    stage           VARCHAR(50),                    -- ETL için adım adı (dim_date vs.)
    status          VARCHAR(20),                    -- RUNNING / SUCCESS / FAILED / WARNING
    rows_inserted   INTEGER,
    rows_rejected   INTEGER,                        -- gelecekte kullanım için
    run_at          TIMESTAMP DEFAULT NOW(),        -- başlangıç
    finished_at     TIMESTAMP,                      -- bitiş (finish_run ile doldurulur)
    duration_sec    NUMERIC(10, 2),
    error_msg       TEXT,
    notes           TEXT                            -- ek bilgi alanı
);

-- Her quality check kuralının sonucunu saklar
CREATE TABLE IF NOT EXISTS quality_check_results (
    check_id        SERIAL PRIMARY KEY,             -- canlı DB ile uyumlu (eskiden result_id)
    run_id          INTEGER REFERENCES pipeline_runs(run_id),
    check_name      VARCHAR(200),
    table_name      VARCHAR(100),
    status          VARCHAR(10),                    -- PASS / WARN / FAIL
    metric_value    NUMERIC,                        -- canlı DB: numeric
    threshold       NUMERIC DEFAULT 0,
    detail          TEXT,
    checked_at      TIMESTAMP DEFAULT NOW()
);

-- ==========================================
-- DİMENSİON TABLOLARI
-- ==========================================

CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER PRIMARY KEY,
    full_date       DATE NOT NULL,
    day             SMALLINT,
    month           SMALLINT,
    quarter         SMALLINT,
    year            SMALLINT,
    day_of_week     VARCHAR(15),                    -- Türkçe gün adı: Pazartesi … Pazar
    is_weekend      BOOLEAN DEFAULT FALSE,
    is_holiday      BOOLEAN DEFAULT FALSE,
    is_ramadan      BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dim_branch (
    branch_sk   SERIAL PRIMARY KEY,                -- canlı DB ile uyumlu (eskiden branch_key)
    branch_id   VARCHAR(20) NOT NULL,
    branch_name VARCHAR(100),
    city        VARCHAR(50),
    region      VARCHAR(50),
    open_date   DATE,
    valid_from  DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_to    DATE,
    is_current  BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dim_member (
    member_key      SERIAL PRIMARY KEY,
    member_id       VARCHAR(20) NOT NULL,
    full_name       VARCHAR(100),
    tc_hash         VARCHAR(64),
    city            VARCHAR(50),
    district        VARCHAR(50),
    age_group       VARCHAR(20),
    income_bracket  VARCHAR(20),
    signup_date     DATE,
    member_status   VARCHAR(20),
    member_segment  VARCHAR(30),                    -- PLACEHOLDER: Faz 1'de NULL. Hafta 7'de K-Means kümeleme ile doldurulacak (bkz. README)
    churn_date      DATE,
    valid_from      DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_to        DATE,
    branch_sk       INTEGER REFERENCES dim_branch(branch_sk),
    is_current      BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dim_plan (
    plan_key            SERIAL PRIMARY KEY,
    plan_id             VARCHAR(20) NOT NULL UNIQUE,
    plan_name           VARCHAR(100),
    plan_type           VARCHAR(20),
    duration_months     INTEGER,
    target_amount       NUMERIC(15, 2),
    monthly_installment NUMERIC(12, 2)
);

-- ==========================================
-- FACT TABLOLARI
-- ==========================================

CREATE TABLE IF NOT EXISTS fact_payments (
    payment_id      VARCHAR(20) PRIMARY KEY,
    member_key      INTEGER REFERENCES dim_member(member_key),
    plan_key        INTEGER REFERENCES dim_plan(plan_key),
    date_key        INTEGER REFERENCES dim_date(date_key),
    installment_no  INTEGER,
    due_amount      NUMERIC(12, 2),
    paid_amount     NUMERIC(12, 2),
    days_late       INTEGER,
    payment_status  VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS fact_lottery (
    lottery_id              VARCHAR(20) PRIMARY KEY,
    member_key              INTEGER REFERENCES dim_member(member_key),
    plan_key                INTEGER REFERENCES dim_plan(plan_key),
    date_key                INTEGER REFERENCES dim_date(date_key),
    lottery_round           INTEGER,
    is_winner               BOOLEAN DEFAULT FALSE,
    cumulative_paid_ratio   NUMERIC(5, 4)
);