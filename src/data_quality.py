import logging

from data_generator import get_db_connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

from config_loader import load_config

def run_check(cur, rule, table, sql, threshold=0):
    cur.execute(sql)
    count = cur.fetchone()[0]

    status = "PASS" if count <= threshold else "FAIL"
    detail = f"{count} sorunlu satır" if count > 0 else "Temiz"

    logger.info(f"[{status}] {rule} — {table}: {detail}")

    return {
        "rule": rule,
        "table": table,
        "status": status,
        "detail": detail
    }


def check_staging(cur):
    results = []

    # 1 NULL members
    results.append(run_check(cur,
        "NULL zorunlu alan (members)",
        "staging.members",
        """
        SELECT COUNT(*) FROM staging.members
        WHERE member_id IS NULL
           OR tc_hash IS NULL
           OR city IS NULL
           OR income IS NULL
           OR signup_date IS NULL
           OR member_status IS NULL
        """
    ))

    results.append(run_check(cur,
        "Duplicate member_id",
        "staging.members",
        """
        SELECT COUNT(*) FROM (
            SELECT member_id
            FROM staging.members
            GROUP BY member_id
            HAVING COUNT(*) > 1
        ) t
        """
    ))

    results.append(run_check(cur,
        "Invalid member_status",
        "staging.members",
        """
        SELECT COUNT(*) FROM staging.members
        WHERE member_status NOT IN ('aktif','gecikmeli','pasif','terk')
        """
    ))

    results.append(run_check(cur,
        "Invalid income",
        "staging.members",
        """
        SELECT COUNT(*) FROM staging.members
        WHERE income <= 0
        """
    ))

    results.append(run_check(cur,
        "Underage members",
        "staging.members",
        """
        SELECT COUNT(*) FROM staging.members
        WHERE DATE_PART('year', AGE(birth_date)) < 18
        """
    ))

    results.append(run_check(cur,
        "NULL payment fields",
        "staging.payments",
        """
        SELECT COUNT(*) FROM staging.payments
        WHERE payment_id IS NULL
           OR subscription_id IS NULL
           OR due_date IS NULL
           OR amount_due IS NULL
        """
    ))
    results.append(run_check(cur,
        "Duplicate payment_id",
        "staging.payments",
        """
        SELECT COUNT(*) FROM (
            SELECT payment_id
            FROM staging.payments
            GROUP BY payment_id
            HAVING COUNT(*) > 1
        ) t
        """
    ))

    results.append(run_check(cur,
        "Negative payment",
        "staging.payments",
        """
        SELECT COUNT(*) FROM staging.payments
        WHERE amount_due <= 0
        """
    ))

    results.append(run_check(cur,
        "Invalid payment_status",
        "staging.payments",
        """
        SELECT COUNT(*) FROM staging.payments
        WHERE payment_status NOT IN ('odendi','gecikmeli','kismi','odenmedi')
        """
    ))

    results.append(run_check(cur,
        "Orphan payments",
        "staging.payments",
        """
        SELECT COUNT(*) FROM staging.payments p
        WHERE NOT EXISTS (
            SELECT 1 FROM staging.subscriptions s
            WHERE s.subscription_id = p.subscription_id
        )
        """
    ))

    results.append(run_check(cur,
        "Invalid plan_type",
        "staging.plans",
        """
        SELECT COUNT(*) FROM staging.plans
        WHERE plan_type NOT IN ('konut','arsa','ticari','arac','isyeri')
        """
    ))

    return results

def check_dwh(cur):
    results = []

    results.append(run_check(cur,
        "subscription → member FK",
        "fact_subscription",
        """
        SELECT COUNT(*) FROM fact_subscription fs
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_member dm
            WHERE dm.member_id = fs.member_id
        )
        """
    ))

    results.append(run_check(cur,
        "subscription → plan FK",
        "fact_subscription",
        """
        SELECT COUNT(*) FROM fact_subscription fs
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_plan dp
            WHERE dp.plan_id = fs.plan_id
        )
        """
    ))

    results.append(run_check(cur,
        "payment → subscription FK",
        "fact_payments",
        """
        SELECT COUNT(*) FROM fact_payments fp
        WHERE NOT EXISTS (
            SELECT 1 FROM fact_subscription fs
            WHERE fs.subscription_id = fp.subscription_id
        )
        """
    ))

    cur.execute("SELECT COUNT(*) FROM fact_payments")
    cnt = cur.fetchone()[0]

    results.append({
        "rule": "Payment volume check",
        "table": "fact_payments",
        "status": "PASS" if cnt >= 100000 else "FAIL",
        "detail": f"{cnt:,} rows"
    })

    return results

def check_dwh(cur):
    results = []

    results.append(run_check(cur,
        "subscription → member FK",
        "fact_subscription",
        """
        SELECT COUNT(*) FROM fact_subscription fs
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_member dm
            WHERE dm.member_id = fs.member_id
        )
        """
    ))

    return results 
if __name__ == "__main__":

    conn = get_db_connection()
    cur = conn.cursor()

    print("Data Quality checks başlıyor...")

    staging_results = check_staging(cur)
    dwh_results = check_dwh(cur)

    print("STAGING:", staging_results)
    print("DWH:", dwh_results)

    cur.close()
    conn.close()