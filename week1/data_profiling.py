import psycopg2
import yaml
import logging

logging.basicConfig(
       level=logging.INFO,
       format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_connection():
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
        db = config["database"]
    return psycopg2.connect(
            host=db["host"],
            port=db["port"],
            database=db["name"],
            user=db["user"],
            password=db["password"]
        )
    
def run_query(cursor, query):
        cursor.execute(query)
        return cursor.fetchall()

def section(title):
     print(f"\n{'='*55}")
     print(f"{title}")
     print(f"{'='*55}")

for row in city_dist:
        print(f"{row[0]:<12}: {row[1]:>6,}")

def main():
    conn = get_connection()
    cursor = conn.cursor()

    try:
        profile_members(cursor)
        profile_plans(cursor)

    except Exception as e:
        logger.error(f"Hata: {e}")
        raise

    finally:
        cursor.close()
        conn.close()
       


      
