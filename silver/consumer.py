#  ETL Bronze → Silver BIG DATA 
import json
import psycopg2
from confluent_kafka import Consumer
from datetime import datetime
import time

print(" SILVER ETL BIG DATA démarré (34k+ transactions)...")

# Connexion Postgres SILVER layer
conn = psycopg2.connect(
    host="localhost",
    dbname="ecommerce_medallion", 
    user="data_architect",
    password="medallion2026"
)
cur = conn.cursor()


cur.execute("""
CREATE TABLE IF NOT EXISTS silver_transactions (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    category VARCHAR(50),
    price NUMERIC CHECK (price >= 0),
    discount NUMERIC CHECK (discount >= 0 AND discount <= 1),
    quantity INTEGER CHECK (quantity > 0),
    payment_method VARCHAR(20),
    total_amount NUMERIC CHECK (total_amount >= 0),
    region VARCHAR(50),
    customer_age INTEGER CHECK (customer_age BETWEEN 16 AND 100),
    order_date DATE, -- <-- Nouvelle colonne pour l'historique
    cleaned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
conn.commit()
print(" Table silver_transactions prête avec colonne order_date")

# Kafka Consumer
c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'silver_etl_group_v3',  
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})
c.subscribe(['raw_transactions'])

count_clean = 0
count_rejected = 0
start_time = time.time()

try:
    while True:
        msg = c.poll(1.0)
        if msg and not msg.error():
            record = json.loads(msg.value())
            
            try:
               
                cleaned = {
                    'order_id': str(record.get('order_id', 'UNKNOWN')).strip()[:50],
                    'customer_id': str(record.get('customer_id', 'UNKNOWN')).strip()[:50],
                    'category': str(record.get('category', 'Unknown')).title()[:50],
                    'price': max(0, float(record.get('price', 0))),
                    'discount': min(1.0, max(0, float(record.get('discount', 0)))),
                    'quantity': max(1, int(record.get('quantity', 1))),
                    'payment_method': str(record.get('payment_method', 'Cash'))[:20],
                    'total_amount': max(0, float(record.get('total_amount', 0))),
                    'region': str(record.get('region', 'Unknown'))[:50],
                    'customer_age': max(16, min(100, int(record.get('customer_age', 30)))),
                    'order_date': record.get('order_date') # <-- On récupère la date du JSON
                }
                
               
                cur.execute("""
                    INSERT INTO silver_transactions 
                    (order_id, customer_id, category, price, discount, quantity, 
                     payment_method, total_amount, region, customer_age, order_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (order_id) DO NOTHING
                """, list(cleaned.values()))
                
                if count_clean % 100 == 0:
                    conn.commit()
                
                count_clean += 1
                if count_clean % 1000 == 0:
                    print(f" SILVER {count_clean:,} OK | Date: {cleaned['order_date']}")
                    
            except (ValueError, KeyError) as e:
                count_rejected += 1
        
        elif msg and msg.error():
            print(f" Kafka error: {msg.error()}")

except KeyboardInterrupt:
    print("\n Arrêt demandé")
finally:
    conn.commit()
    c.close()
    cur.close()

    conn.close()
