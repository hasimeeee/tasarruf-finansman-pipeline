# Tasarruf Finansman Data Pipeline

Tasarruf finansman şirketleri için sentetik veri üreten ve bunu bir star schema'ya dönüştüren ETL pipeline'ı.
Proje, veri mühendisliği öğrenme amaçlıdır; her hafta yeni bir kavram eklenmektedir.

---

## Proje Yapısı

```
tasarruf_pipeline/
├── config.yaml              # Veritabanı bağlantı ayarları
├── .env                     # Gizli bilgiler — .gitignore'a ekle (bkz. Güvenlik)
├── config_loader.py         # config.yaml + .env birleştirici
├── data_generator.py        # Sentetik veri üretimi → staging
├── data_quality.py          # Kalite kontrolleri + satır kaybı raporu
├── etl_pipeline.py          # Staging → Star Schema ETL
├── transformers.py          # Dönüşüm fonksiyonları
├── sql/
│   ├── staging.sql          # Staging schema ve tabloları
│   └── ddl.sql              # DWH (public schema) star schema tabloları
└── utils/
    └── logger.py            # Opsiyonel özel logger
```

---

## Haftalık İlerleme

| Hafta | Kapsam |
|-------|--------|
| 1 | Sentetik veri üretimi, staging schema, data profiling |
| 2 | ETL pipeline, star schema, SCD Type 2 (dim_member), cumulative_paid_ratio |
| 3-4 | Schema tutarlılığı, UPSERT idempotency, dim_branch, data quality raporu |
| 7 | K-Means ile member_segment (dim_member) |

---

## Kurulum

### 1. Gereksinimler

- Python 3.9+
- PostgreSQL 14+ (Docker ile kolayca başlatılabilir)
- pip paketleri:

```bash
pip install psycopg2-binary faker python-dateutil pyyaml python-dotenv
```

### 2. Veritabanını Başlat (Docker)

```bash
docker-compose up -d
```

Veya doğrudan PostgreSQL varsa:

```bash
psql -U postgres -c "CREATE DATABASE tasarruf_db;"
```

### 3. Schema ve Tabloları Oluştur

```bash
psql -U postgres -d tasarruf_db -f sql/staging.sql
psql -U postgres -d tasarruf_db -f sql/ddl.sql
```

### 4. Konfigürasyon

`config.yaml` örneği:

```yaml
database:
  host: localhost
  port: 5432
  name: tasarruf_db
  user: postgres
  password: "${DB_PASSWORD}"   # .env'den okunur

data_generation:
  num_members: 15000
```

`.env` dosyası oluştur (asla commit'leme):

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=tasarruf_db
DB_USER=postgres
DB_PASSWORD=gercek_sifre_buraya
```

`.gitignore`'a ekle:

```
.env
config.yaml
```

---

## Çalıştırma

### Adım 1 — Sentetik veri üret ve staging'e yaz

```bash
python data_generator.py
```

Üretilen veriler:
- ~15.000 üye (kasıtlı ~%8 NULL tc_hash, ~%3 duplike)
- 5 plan
- ~360.000+ ödeme kaydı
- ~10.000 kura kaydı
- 15 şube (Hafta 3-4)

### Adım 2 — ETL pipeline'ı çalıştır

```bash
python etl_pipeline.py
```

Sırayla yüklenir: `dim_date` → `dim_plan` → `dim_member` (SCD2) → `fact_payments` → `fact_lottery`

Her adımın süresi ve satır sayısı `pipeline_runs` tablosuna kaydedilir.

### Adım 3 — Data quality kontrol

```bash
python data_quality.py
```

Çıktı: staging ve DWH için PASS/FAIL listesi + satır kaybı raporu (NULL filtresi / duplike / FK mismatch ayrımı).

---

## Star Schema

```
           dim_date
               │
dim_member ─── fact_payments ─── dim_plan
               │
           fact_lottery
               │
           dim_branch (Hafta 3-4)
```

**dim_member** SCD Type 2 ile takip edilir: statü değiştiğinde (aktif → gecikmeli → terk) eski kayıt `valid_to` ile kapatılır, yeni kayıt eklenir.

---

## Güvenlik Notu

`config.yaml`'daki `password` alanı şu an düz metin. Gerçek projede:

1. `.env` dosyasına taşı
2. `.gitignore`'a ekle
3. `config_loader.py` zaten `os.getenv` ile okuyacak şekilde yazılmış

---

## Bilinen Sınırlamalar / Hafta 3-4 Planı

- `TRUNCATE + INSERT` stratejisi full reload yapar. Hafta 3'te `INSERT ON CONFLICT DO UPDATE` (UPSERT) ile gerçek idempotency'ye geçilecek.
- `dim_branch` tablosu oluşturuldu ancak pipeline'a henüz entegre edilmedi.
- SCD Type 2 statü geçiş simülasyonu (aktif → gecikmeli → terk) Hafta 3-4'te gerçek zaman serisi ile canlandırılacak.