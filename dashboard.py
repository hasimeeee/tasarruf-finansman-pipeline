import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

# ── Sayfa ayarları ──────────────────────────────────────────
st.set_page_config(
    page_title="Tasarruf Finansman Dashboard",
    page_icon="🏠",
    layout="wide"
)

# ── Bağlantı ────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    return create_engine(
        "postgresql://neondb_owner:npg_X9aygcQ3EHlZ@ep-super-term-al9k62e1.c-3.eu-central-1.aws.neon.tech/neondb?sslmode=require"
    )

engine = get_engine()

@st.cache_data(ttl=300)
def query(sql):
    return pd.read_sql(sql, engine)

# ── Başlık ──────────────────────────────────────────────────
st.title("🏠 Tasarruf Finansman Analiz Dashboard")
st.markdown("---")

# ── Filtreler (sidebar) ─────────────────────────────────────
st.sidebar.header("🔍 Filtreler")

plan_tipleri = query("SELECT DISTINCT plan_type FROM dim_plan ORDER BY 1")
secili_plan = st.sidebar.multiselect(
    "Plan Tipi",
    options=plan_tipleri["plan_type"].tolist(),
    default=plan_tipleri["plan_type"].tolist()
)

sehirler = query("SELECT DISTINCT city FROM dim_member WHERE city IS NOT NULL ORDER BY 1")
secili_sehir = st.sidebar.multiselect(
    "Şehir",
    options=sehirler["city"].tolist(),
    default=sehirler["city"].tolist()[:5]
)

yillar = query("SELECT DISTINCT year FROM dim_date ORDER BY 1")
yil_listesi = yillar["year"].tolist()
secili_yil = st.sidebar.select_slider(
    "Yıl",
    options=yil_listesi,
    value=(min(yil_listesi), max(yil_listesi))
)

# ── KPI Kartları ────────────────────────────────────────────
st.subheader("📊 Temel KPI'lar")

kpi = query("""
    SELECT
        COUNT(DISTINCT m.member_id)                                              AS toplam_uye,
        ROUND((100.0 * SUM(CASE WHEN m.member_status = 'aktif' THEN 1 ELSE 0 END)
              / COUNT(*))::NUMERIC, 1)                                           AS aktif_oran,
        ROUND((SUM(p.paid_amount) / 1000000.0)::NUMERIC, 2)                     AS toplam_tahsilat_m,
        ROUND(AVG(NULLIF(p.days_late, 0))::NUMERIC, 1)                          AS ort_gecikme,
        ROUND((100.0 * SUM(CASE WHEN m.member_status = 'pasif' THEN 1 ELSE 0 END)
              / COUNT(*))::NUMERIC, 1)                                           AS churn_orani
    FROM dim_member m
    LEFT JOIN fact_payments p ON m.member_key = p.member_key
    WHERE m.is_current = TRUE
""")

kura = query("""
    SELECT ROUND(AVG(aylik_oran)::NUMERIC, 1) AS kura_orani
    FROM (
        SELECT ROUND((100.0 * COUNT(DISTINCT CASE WHEN p.payment_status = 'zamaninda'
              THEN p.member_key END) / NULLIF(COUNT(DISTINCT p.member_key), 0))::NUMERIC, 1) AS aylik_oran
        FROM fact_payments p
        JOIN dim_date d ON p.date_key = d.date_key
        GROUP BY d.year, d.month
    ) alt
""")

k = kpi.iloc[0]
col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric("👥 Toplam Üye",       f"{int(k['toplam_uye']):,}")
col2.metric("✅ Aktif Oran",        f"%{k['aktif_oran']}")
col3.metric("💰 Toplam Tahsilat",  f"₺{k['toplam_tahsilat_m']}M")
col4.metric("⏱️ Ort. Gecikme",     f"{k['ort_gecikme']} gün")
col5.metric("🚪 Churn Oranı",      f"%{k['churn_orani']}")
col6.metric("🎰 Kura Oranı",       f"%{kura.iloc[0]['kura_orani']}")

st.markdown("---")

# ── Grafik 1: Aylık Tahsilat Trendi ─────────────────────────
st.subheader("📈 Aylık Tahsilat Trendi")

trend = query(f"""
    SELECT
        d.year || '-' || LPAD(d.month::text, 2, '0') AS ay,
        ROUND((SUM(p.paid_amount) / 1000000.0)::NUMERIC, 2) AS tahsilat_m
    FROM fact_payments p
    JOIN dim_date d   ON p.date_key = d.date_key
    JOIN dim_plan pl  ON p.plan_key = pl.plan_key
    WHERE d.year BETWEEN {secili_yil[0]} AND {secili_yil[1]}
      AND pl.plan_type = ANY(ARRAY{secili_plan})
    GROUP BY d.year, d.month
    ORDER BY d.year, d.month
""")

fig1 = px.line(trend, x="ay", y="tahsilat_m",
               labels={"ay": "Ay", "tahsilat_m": "Tahsilat (₺M)"},
               markers=True, color_discrete_sequence=["#2563eb"])
fig1.update_layout(xaxis_tickangle=-45)
st.plotly_chart(fig1, use_container_width=True)

st.markdown("---")
col_a, col_b = st.columns(2)

# ── Grafik 2: Plan Dağılımı ──────────────────────────────────
with col_a:
    st.subheader("🏗️ Plan Tipi Dağılımı")
    plan_dagilim = query(f"""
        SELECT pl.plan_type, COUNT(*) AS uye_sayisi
        FROM fact_payments p
        JOIN dim_plan pl   ON p.plan_key = pl.plan_key
        JOIN dim_member m  ON p.member_key = m.member_key AND m.is_current = TRUE
        WHERE pl.plan_type = ANY(ARRAY{secili_plan})
          AND m.city = ANY(ARRAY{secili_sehir})
        GROUP BY pl.plan_type
    """)
    fig2 = px.pie(plan_dagilim, names="plan_type", values="uye_sayisi",
                  color_discrete_sequence=px.colors.qualitative.Set2)
    st.plotly_chart(fig2, use_container_width=True)

# ── Grafik 3: Gecikme Dağılımı ───────────────────────────────
with col_b:
    st.subheader("⏱️ Gecikme Dağılımı (Plan Tipine Göre)")
    gecikme = query(f"""
        SELECT pl.plan_type, p.days_late
        FROM fact_payments p
        JOIN dim_plan pl ON p.plan_key = pl.plan_key
        WHERE p.days_late > 0
          AND pl.plan_type = ANY(ARRAY{secili_plan})
        LIMIT 50000
    """)
    fig3 = px.box(gecikme, x="plan_type", y="days_late",
                  labels={"plan_type": "Plan Tipi", "days_late": "Gecikme (Gün)"},
                  color="plan_type",
                  color_discrete_sequence=px.colors.qualitative.Set2)
    st.plotly_chart(fig3, use_container_width=True)

st.markdown("---")

# ── Grafik 4: Şehir Bazlı Tahsilat Heatmap ───────────────────
st.subheader("🗺️ Şehir × Yıl Tahsilat Heatmap")

heatmap_data = query(f"""
    SELECT m.city, d.year,
           ROUND((SUM(p.paid_amount) / 1000000.0)::NUMERIC, 2) AS tahsilat_m
    FROM fact_payments p
    JOIN dim_member m  ON p.member_key = m.member_key AND m.is_current = TRUE
    JOIN dim_date d    ON p.date_key = d.date_key
    WHERE m.city = ANY(ARRAY{secili_sehir})
      AND d.year BETWEEN {secili_yil[0]} AND {secili_yil[1]}
    GROUP BY m.city, d.year
    ORDER BY m.city, d.year
""")

if not heatmap_data.empty:
    pivot = heatmap_data.pivot(index="city", columns="year", values="tahsilat_m").fillna(0)
    fig4 = px.imshow(pivot, text_auto=True, aspect="auto",
                     labels={"x": "Yıl", "y": "Şehir", "color": "Tahsilat (₺M)"},
                     color_continuous_scale="Blues")
    st.plotly_chart(fig4, use_container_width=True)

st.markdown("---")

# ── Grafik 5: Cohort Retention ───────────────────────────────
st.subheader("👥 Cohort Retention Analizi")

cohort = query("""
    WITH cohort_base AS (
        SELECT
            m.member_key,
            DATE_TRUNC('month', m.signup_date) AS cohort_ay,
            DATE_TRUNC('month', d.full_date)   AS odeme_ay
        FROM fact_payments p
        JOIN dim_member m ON p.member_key = m.member_key AND m.is_current = TRUE
        JOIN dim_date d   ON p.date_key = d.date_key
    ),
    cohort_size AS (
        SELECT cohort_ay, COUNT(DISTINCT member_key) AS cohort_uye
        FROM cohort_base GROUP BY 1
    ),
    retention AS (
        SELECT
            cohort_ay,
            EXTRACT(MONTH FROM AGE(odeme_ay, cohort_ay))::int AS ay_no,
            COUNT(DISTINCT member_key) AS aktif_uye
        FROM cohort_base
        GROUP BY 1, 2
    )
    SELECT
        TO_CHAR(r.cohort_ay, 'YYYY-MM')                        AS cohort,
        r.ay_no,
        ROUND((100.0 * r.aktif_uye / cs.cohort_uye)::NUMERIC, 1) AS retention_pct
    FROM retention r
    JOIN cohort_size cs ON r.cohort_ay = cs.cohort_ay
    WHERE r.ay_no BETWEEN 0 AND 12
    ORDER BY 1, 2
""")

if not cohort.empty:
    pivot_c = cohort.pivot(index="cohort", columns="ay_no", values="retention_pct").fillna(0)
    fig5 = px.imshow(pivot_c, text_auto=True, aspect="auto",
                     labels={"x": "Ay (Üyelikten Sonra)", "y": "Cohort", "color": "Retention %"},
                     color_continuous_scale="RdYlGn")
    st.plotly_chart(fig5, use_container_width=True)

st.markdown("---")

# ── Grafik 6: Gecikme Histogram ──────────────────────────────
st.subheader("📊 Gecikme Dağılımı (Histogram)")

histogram_data = query(f"""
    SELECT p.days_late, pl.plan_type
    FROM fact_payments p
    JOIN dim_plan pl ON p.plan_key = pl.plan_key
    WHERE p.days_late > 0
      AND pl.plan_type = ANY(ARRAY{secili_plan})
    LIMIT 50000
""")

fig6 = px.histogram(
    histogram_data, x="days_late", color="plan_type",
    nbins=50,
    labels={"days_late": "Gecikme (Gün)", "count": "Ödeme Sayısı"},
    barmode="overlay", opacity=0.7,
    color_discrete_sequence=px.colors.qualitative.Set2
)
st.plotly_chart(fig6, use_container_width=True)

st.caption("FuzulEv AI & Data Departmanı — Stajyer Proje Ödevi Hafta 6")