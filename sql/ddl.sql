-- ==========================================
-- STAGING TABLOLARI
-- ==========================================

-- Staging: Üyeler
CREATE TABLE IF NOT EXISTS staging_members (
    id              SERIAL PRIMARY KEY,
    member_id       VARCHAR(20),
    full_name       VARCHAR(100),
    tc_hash         VARCHAR(64),
    city            VARCHAR(50),
    district        VARCHAR(50),
    birth_date      DATE,
    income          NUMERIC(12,2),
    signup_date     DATE,
    member_status   VARCHAR(20),
    phone           VARCHAR(30),
    email           VARCHAR(100),
    loaded_at       TIMESTAMP DEFAULT NOW()
);

-- Staging: Planlar
CREATE TABLE IF NOT EXISTS staging_plans (
    id                  SERIAL PRIMARY KEY,
    plan_id             VARCHAR(20),
    plan_name           VARCHAR(100),
    plan_type           VARCHAR(20),
    duration_months     INT,
    target_amount       NUMERIC(12,2),
    monthly_installment NUMERIC(12,2),
    loaded_at           TIMESTAMP DEFAULT NOW()
);

-- Staging: Ödemeler
CREATE TABLE IF NOT EXISTS staging_payments (
    id                  SERIAL PRIMARY KEY,
    payment_id          VARCHAR(20),
    member_id           VARCHAR(20),
    plan_id             VARCHAR(20),
    installment_no      INT,
    due_date            DATE,
    due_amount          NUMERIC(12,2),
    paid_amount         NUMERIC(12,2),
    paid_date           DATE,
    payment_status      VARCHAR(20),
    loaded_at           TIMESTAMP DEFAULT NOW()
);

-- Staging: Kura
CREATE TABLE IF NOT EXISTS staging_lottery (
    id                  SERIAL PRIMARY KEY,
    lottery_id          VARCHAR(20),
    member_id           VARCHAR(20),
    plan_id             VARCHAR(20),
    lottery_date        DATE,
    lottery_round       INT,
    is_winner           BOOLEAN,
    loaded_at           TIMESTAMP DEFAULT NOW()
);