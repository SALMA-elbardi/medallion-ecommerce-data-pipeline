import psycopg2

try:
    conn = psycopg2.connect(
        host="localhost", 
        dbname="ecommerce_medallion", 
        user="data_architect", 
        password="medallion2026"
    )
    cur = conn.cursor()

    # Nettoyage de toutes les couches
    print("Nettoyage en cours...")
    cur.execute("""
        DROP TABLE IF EXISTS gold_customer_segments CASCADE;
        DROP TABLE IF EXISTS gold_customer_rfm CASCADE;
        DROP TABLE IF EXISTS gold_daily_revenue CASCADE;
        DROP TABLE IF EXISTS silver_transactions CASCADE;
    """)
    
    conn.commit()
    print("✅ Succès : Les tables Silver et Gold ont été supprimées.")

except Exception as e:
    print(f" Erreur : {e}")
finally:
    if conn:
        cur.close()
        conn.close()