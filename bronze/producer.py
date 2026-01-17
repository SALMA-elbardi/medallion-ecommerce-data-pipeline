import pandas as pd
import json
from confluent_kafka import Producer
import time

# Config Kafka optimisée BIG DATA
conf = {
    'bootstrap.servers': 'localhost:9092',
    'linger.ms': 100,      # Batch 100ms (perf)
    'batch.size': 16384,   # 16KB batch
    'compression.type': 'snappy'
}

# Vérif dataset
csv_path = "D:/medallion_ecommerce/data/ecommerce_transactions.csv"
print(" Chargement des donnees ")
df = pd.read_csv(csv_path)

print(f" {len(df):,} transactions | Colonnes: {list(df.columns)}")
print(df.head(2))

# Kafka producer optimisé
p = Producer(conf)

def delivery(err, msg):
    if err:
        print(f" ERREUR: {err}")
    else:
        global count
        count += 1
        if count % 1000 == 0:
            print(f"📡 {count:,}/{total} → Kafka BRONZE")

# BIG DATA : TOUT LE DATASET (pas .head(1000))
total = len(df)
count = 0

print(f" STREAMING COMPLET {total:,} transactions vers Kafka.")

for i, row in df.iterrows():  # ← CHANGEMENT : df.iterrows() pas .head(1000)
    record = row.to_dict()
    record['_bronze_ts'] = time.time()
    p.produce('raw_transactions', key=str(i), value=json.dumps(record), callback=delivery)

# Flush final + stats
p.flush()
print(f" {total:,} transactions → Kafka BRONZE COMPLET !")
print(f" Temps: {time.time() - start_time:.1f}s")
