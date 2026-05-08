"""
Statü geçiş simülasyonu: aktif → gecikmeli → terk
dim_member'da tarihsel kayıt görünmeli.
"""

import sys
import os

sys.path.append(os.path.dirname(__file__))
from etl_pipeline import get_conn, load_dim_member_scd2
from logger import get_logger          # utils/ klasörü yok — logger.py kök dizinde

log = get_logger("simulasyon")


def simulate_status_transitions():
    conn = get_conn()
    cur = conn.cursor()
    member_id = "M00002"

    log.info(f"=== Statü geçiş simülasyonu başlıyor: {member_id} ===")

    # Temizle — önceki çalışmadan kalan kayıtları sil
    # FK sırası önemli: önce fact tabloları, sonra dim
    cur.execute("""
        DELETE FROM fact_payments
        WHERE member_key IN (
            SELECT member_key FROM dim_member WHERE member_id = %s
        )
    """, (member_id,))
    cur.execute("""
        DELETE FROM fact_lottery
        WHERE member_key IN (
            SELECT member_key FROM dim_member WHERE member_id = %s
        )
    """, (member_id,))
    cur.execute("DELETE FROM dim_member WHERE member_id = %s", (member_id,))
    conn.commit()

    # ADIM 1: aktif
    log.info("Adım 1: aktif")
    cur.execute("UPDATE staging.members SET member_status = 'aktif' WHERE member_id = %s", (member_id,))
    conn.commit()
    load_dim_member_scd2(conn)

    # ADIM 2: gecikmeli
    log.info("Adım 2: gecikmeli")
    cur.execute("UPDATE staging.members SET member_status = 'gecikmeli' WHERE member_id = %s", (member_id,))
    conn.commit()
    load_dim_member_scd2(conn)

    # ADIM 3: terk
    log.info("Adım 3: terk")
    cur.execute("UPDATE staging.members SET member_status = 'terk' WHERE member_id = %s", (member_id,))
    conn.commit()
    load_dim_member_scd2(conn)

    # Sonucu göster
    cur.execute("""
        SELECT member_id, member_status, valid_from, valid_to, is_current
        FROM dim_member
        WHERE member_id = %s
        ORDER BY valid_from
    """, (member_id,))
    rows = cur.fetchall()

    log.info(f"=== {member_id} için dim_member geçmişi ===")
    for row in rows:
        log.info(f"  {row[0]} | {row[1]:10} | {row[2]} → {row[3]} | is_current={row[4]}")

    conn.close()


if __name__ == "__main__":
    simulate_status_transitions()