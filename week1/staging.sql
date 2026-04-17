CREATE SCHEMA IF NOT EXISTS staging;
-- ==========================================
-- 1. STAGING TABLOLARI
-- ==========================================

-- Staging: Üyeler
CREATE TABLE IF NOT EXISTS staging.members (
    id              SERIAL PRIMARY KEY,
    member_id       VARCHAR(20) UNIQUE,
    full_name       VARCHAR(100),
    tc_hash         VARCHAR(64),        -- NULL olabilir (kirli veri senaryosu)
    city            VARCHAR(50),
    district        VARCHAR(50),
    birth_date      DATE,
    income          NUMERIC(12, 2),
    signup_date     DATE,
    member_status   VARCHAR(20),        -- aktif / gecikmeli / pasif / terk
    phone           VARCHAR(30),
    email           VARCHAR(100),
    loaded_at       TIMESTAMP DEFAULT NOW()
);

-- Staging: Planlar
CREATE TABLE IF NOT EXISTS staging.plans (
    id                  SERIAL PRIMARY KEY,
    plan_id             VARCHAR(20),
    plan_name           VARCHAR(100),
    plan_type           VARCHAR(20),        -- konut / arsa / ticari / arac / isyeri
    duration_months     INTEGER,
    target_amount       NUMERIC(15, 2),
    monthly_installment NUMERIC(12, 2),
    loaded_at           TIMESTAMP DEFAULT NOW()
);

-- Staging: Abonelikler
CREATE TABLE IF NOT EXISTS staging.subscriptions (
    id                  SERIAL PRIMARY KEY,
    subscription_id     VARCHAR(15),
    member_id           VARCHAR(20),
    plan_id             VARCHAR(20),
    start_date          DATE,
    expected_end_date   DATE,
    kura_date           DATE,               -- NULL ise kura kazanılmadı
    kura_won            BOOLEAN DEFAULT FALSE,
    subscription_status VARCHAR(20),        -- aktif / beklemede / terk
    loaded_at           TIMESTAMP DEFAULT NOW()
);

-- Staging: Ödemeler
CREATE TABLE IF NOT EXISTS staging.payments (
    id                  SERIAL PRIMARY KEY,
    payment_id          VARCHAR(20),
    subscription_id     VARCHAR(15),
    member_id           VARCHAR(20),
    due_date            DATE,
    payment_date        DATE,               -- NULL ise ödenmedi
    amount_due          NUMERIC(12, 2),
    amount_paid         NUMERIC(12, 2),
    days_late           INTEGER,            -- NULL ise ödenmedi; negatif = erken ödeme
    payment_status      VARCHAR(20),        -- odendi / gecikmeli / kismi / odenmedi
    loaded_at           TIMESTAMP DEFAULT NOW()
);

-- Staging: Kura Çekilişleri
CREATE TABLE IF NOT EXISTS staging.lottery (
    id              SERIAL PRIMARY KEY,
    lottery_id      VARCHAR(20),
    member_id       VARCHAR(20),
    plan_id         VARCHAR(20),
    subscription_id VARCHAR(15),
    lottery_date    DATE,
    lottery_round   INTEGER,
    is_winner       BOOLEAN DEFAULT FALSE,
    loaded_at       TIMESTAMP DEFAULT NOW()
);