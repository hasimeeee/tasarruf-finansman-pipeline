import pytest
import sys
import os
from datetime import date

sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))
from etl_pipeline import get_conn, load_dim_member_scd2
@pytest.fixture
def conn():
    connection = get_conn()
    yield connection
    connection.rollback()
    connection.close()

def test_yeni_uye_eklenir(conn):
    cur = conn.cursor()
    
    # Sahte yeni üye staging'e ekle
    cur.execute("""
        INSERT INTO staging_members 
        (member_id, full_name, tc_hash, city, district,
         birth_year, income, signup_date, status)
        VALUES 
        ('TEST001', 'Test Kisi', 'abc123hash', 'Istanbul', 'Kadikoy',
         1990, 50000, '2024-01-01', 'aktif')
        ON CONFLICT DO NOTHING
    """)
    conn.commit()
    
    # SCD2 çalıştır
    load_dim_member_scd2(conn)
    
    # dim_member'da var mı?
    cur.execute("""
        SELECT COUNT(*) FROM dim_member 
        WHERE member_id = 'TEST001' AND is_current = TRUE
    """)
    count = cur.fetchone()[0]
    
    assert count == 1, "Yeni üye dim_member'a eklenmedi!"