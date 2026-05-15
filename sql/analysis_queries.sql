--1.Aylık tahsilat trendi + MoM büþyüme oranı (LAG window function)
with month as (
select
dd.year,
dd.month,
sum(fp.paid_amount) as toplam_tahsilat
from fact_payments fp 
join dim_date dd on fp.date_key = dd.date_key 
group by dd.year, dd.month
order by dd.year, dd.month
)
select 
year || '-' || lpad(month::text, 2, '0') as ay,
toplam_tahsilat,
lag(toplam_tahsilat) over (order by year, month) as gecen_ay,
round(
(toplam_tahsilat - lag(toplam_tahsilat) over (order by year, month))
/ lag(toplam_tahsilat) over (order by year, month) * 100, 2) as buyume_yuzdesi
from month;

-- 2.Tahsilat oranı: plan tipi, şehir ve şube bazlı
with tahsilat as (
select 
dp.plan_type as plan_tipi,
dm.city  as sehir,
db.branch_name as sube,
sum(fp.paid_amount) as odenen,
sum(fp.due_amount) as vadesi_gelen,
round(
sum(fp.paid_amount) / nullif(sum(fp.due_amount), 0) * 100
        , 2)                                                AS tahsilat_orani
from fact_payments fp
join dim_plan    dp on fp.plan_key   = dp.plan_key
join dim_member  dm on fp.member_key = dm.member_key
join dim_branch  db on dm.branch_sk  = db.branch_sk  
group by dp.plan_type, dm.city, db.branch_name
)
select
plan_tipi,
sehir,
sube,
odenen,
vadesi_gelen,
tahsilat_orani,
rank() over (order by tahsilat_orani desc) as siralama
from tahsilat
order by tahsilat_orani desc;

--3.Üye cohort analizi: kayıt ayına göre 6/12/18 ay retention
with cohortlar as(
select
dm.member_key,
DATE_TRUNC('month', dm.signup_date) AS cohort_ay,
DATE_TRUNC('month', dd.full_date)    AS odeme_ay
from fact_payments fp
join dim_member dm on fp.member_key = dm.member_key
join dim_date dd on fp.date_key = dd.date_key
),
cohort_diff as(
select 
member_key,
cohort_ay,
odeme_ay,
extract(year from age(odeme_ay, cohort_ay)) * 12 +
extract(month from age (odeme_ay, cohort_ay)) as ay_farki
from cohortlar
),
cohort_size as (
select 
cohort_ay,
count(distinct member_key) as toplam_uye
from cohortlar
group by cohort_ay
),
retention_counts as (
select
cohort_ay,
ay_farki,
count(distinct member_key) as aktif_uye
from cohort_diff
where ay_farki in (6, 12, 18)
group by cohort_ay, ay_farki
),
retention_rates as (
select
rc.cohort_ay,
cs.toplam_uye,
rc.ay_farki,
rc.aktif_uye,
round(
(rc.aktif_uye * 100.0 / NULLIF(cs.toplam_uye, 0))::numeric, 1
) as retention_orani
from retention_counts rc
join cohort_size cs on rc.cohort_ay = cs.cohort_ay
)
select 
cohort_ay,
toplam_uye,
max(case when ay_farki = 6 then aktif_uye end) as "6ay_aktif",
max(case when ay_farki = 12 then aktif_uye end) as "12ay_aktif",
max(case when ay_farki = 18 then aktif_uye end) as "18ay_aktif",

max(case when ay_farki = 6 then retention_orani end) as "6ay_retention_%",
max(case when ay_farki = 12 then retention_orani end) as "12ay_retention_%",
max(case when ay_farki = 18 then retention_orani end) as "18ay_retention_%"
from retention_rates
group by cohort_ay, toplam_uye
order by cohort_ay ;       


--4.Gecikme analizi: ortalama gecikme trendi, mevsimsel etki (Ramazan, yaz, yıl sonu
with donemler as (
select
dd.year as yil,
fp.days_late,
case 
	when is_ramadan = true then 'ramazan'
	when is_holiday = true then 'tatil'
	when is_weekend = true then 'haftasonu'
	ELSE 'normal'
end
as donem
from fact_payments fp 
join dim_date dd on fp.date_key = dd.date_key 
)
select 
yil,
donem,
count(*)  as taksit_sayisi,
round(avg(days_late),2) as ort_gecikme
from donemler 
GROUP BY yil, donem 
order by yil, donem; 

--5.Kura analizi: kazanma oranı vs. ödeme düzenliliği korelasyonu
select
CASE 
    WHEN cumulative_paid_ratio < 0.25 THEN 'cok_duzensiz'
    WHEN cumulative_paid_ratio < 0.50 THEN 'duzensiz'
    WHEN cumulative_paid_ratio < 0.75 THEN 'duzenli'
    ELSE 'cok_duzenli'
END AS odeme_grubu,
count(*) as katilim_sayisi,
sum(case when is_winner then 1 else 0 end) as kazanan_sayisi,
round(avg(case when is_winner then 1.0 else 0 end) * 100, 2) as kazanma_yuzdesi
from fact_lottery
group by odeme_grubu 
order by kazanma_yuzdesi desc;

--6.Şube ranking: RANK/DENSE_RANK ile performans sıralaması
with subeler as(
select
dm.city as sehir,
db.branch_name as sube,
SUM(fp.paid_amount) as odenen,
SUM(due_amount) as toplam
from fact_payments fp
join dim_member dm on fp.member_key = dm.member_key 
join dim_branch db on dm.branch_sk = db.branch_sk 
group by dm.city, db.branch_name 
),
tahsilat AS (
select 
sehir,
sube,
round(odenen / nullif(toplam,0) * 100, 2) as tahsilat_orani
from subeler
)
select
sehir,
sube,
tahsilat_orani,
RANK() OVER (ORDER BY tahsilat_orani DESC) AS rank_sira,
DENSE_RANK() OVER (ORDER BY tahsilat_orani DESC) AS dense_rank_sira
FROM tahsilat
ORDER BY tahsilat_orani DESC;

-- 7. Churn profili: terk edenlerin ortalama ödeme süresi + son ödeme öncesi gecikme pattern'i
with sure as (
select
dm.member_key,
case when dm.is_current = false then 'terk_etti' else 'aktif' end as churn_durum,
extract(year from age(max(dd.full_date), min(dd.full_date))) * 12 +
extract(month from age(max(dd.full_date), min(dd.full_date))) as aktif_ay_sayisi
from fact_payments fp
join dim_member dm on fp.member_key = dm.member_key
join dim_date   dd on fp.date_key   = dd.date_key
group by dm.member_key, dm.is_current
),
ort_sure as (
select
churn_durum,
round(avg(aktif_ay_sayisi), 1) as ort_aktif_sure_ay,
count(*)                        as uye_sayisi
from sure
group by churn_durum
),
sirali as (
select
dm.member_key,
case when dm.is_current = false then 'terk_etti' else 'aktif' end as churn_durum,
fp.days_late,
fp.payment_status,
row_number() over (
partition by fp.member_key
order by dd.full_date desc
) as odeme_sirasi
from fact_payments fp
join dim_member dm on fp.member_key = dm.member_key
join dim_date   dd on fp.date_key   = dd.date_key
),
gecikme as (
select
churn_durum,
odeme_sirasi,
round(avg(days_late), 2)                                                              as ort_gecikme,
round(sum(case when payment_status = 'odenmedi' then 1 else 0 end) * 100.0 / count(*), 2) as odenmedi_oran
from sirali
where odeme_sirasi <= 6
group by churn_durum, odeme_sirasi
)
select
    g.churn_durum,
    os.ort_aktif_sure_ay,
    os.uye_sayisi,
    g.odeme_sirasi,
    g.ort_gecikme,
    g.odenmedi_oran
from gecikme g
join ort_sure os on g.churn_durum = os.churn_durum
order by g.churn_durum, g.odeme_sirasi;