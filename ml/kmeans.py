import pandas as pd
import psycopg2
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score

print(" KMeans ML sur TOUS les clients (36k transactions)...")

# Connexion PostgreSQL
conn = psycopg2.connect(
    host="localhost", 
    dbname="ecommerce_medallion", 
    user="data_architect", 
    password="medallion2026"
)
cur = conn.cursor()

# 1. LECTURE RFM COMPLET (AVEC customer_id)
df_rfm = pd.read_sql("""
    SELECT customer_id, frequency, monetary_value, recency_days 
    FROM gold_customer_rfm
""", conn)
print(f" {len(df_rfm)} clients uniques chargés (BIG DATA)")

# 2. KMEANS 3 clusters (features RFM)
scaler = StandardScaler()
X_scaled = scaler.fit_transform(df_rfm[['frequency', 'monetary_value', 'recency_days']])
kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
clusters = kmeans.fit_predict(X_scaled)



# 3. Silhouette Score (Correction pour économiser la mémoire)
# On utilise un échantillon de 2000 clients pour calculer le score
if len(X_scaled) > 2000:
    sil_score = silhouette_score(X_scaled, clusters, sample_size=2000, random_state=42)
else:
    sil_score = silhouette_score(X_scaled, clusters)

print(f" KMeans Silhouette Score : {sil_score:.3f}")



# 4. Ajout clusters
df_rfm['cluster'] = clusters
df_rfm['cluster_name'] = df_rfm['cluster'].map({0: 'VIP', 1: 'Growth', 2: 'Lost'})

# 5. INSERT DIRECT SQL (FIX customer_id)
cur.execute("DROP TABLE IF EXISTS gold_customer_segments;")
cur.execute("""
    CREATE TABLE gold_customer_segments (
        customer_id VARCHAR(50),
        frequency INTEGER,
        monetary_value NUMERIC,
        recency_days INTEGER,
        cluster INTEGER,
        cluster_name VARCHAR(20)
    )
""")

# INSERT BATCH (7903 clients)
for idx, row in df_rfm.iterrows():
    cur.execute("""
        INSERT INTO gold_customer_segments 
        (customer_id, frequency, monetary_value, recency_days, cluster, cluster_name)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        row['customer_id'], row['frequency'], row['monetary_value'], 
        row['recency_days'], row['cluster'], row['cluster_name']
    ))

conn.commit()
print(f" {len(df_rfm):,} clients → 3 clusters ML sauvés (Silhouette {sil_score:.3f})")

# Stats clusters
cluster_stats = df_rfm['cluster_name'].value_counts()
print(" Distribution clusters:")
for name, count in cluster_stats.items():
    print(f"   {name}: {count} ({count/len(df_rfm)*100:.1f}%)")

cur.close()
conn.close()
