import psycopg2

# Connexion
conn = psycopg2.connect(
    host="localhost", dbname="ecommerce_medallion", 
    user="data_architect", password="medallion2026"
)
cur = conn.cursor()

#  SQL GOLD 
sql_gold = """
-- 1. REVENUE KPIs (Utilise maintenant order_date au lieu de cleaned_at)
DROP TABLE IF EXISTS gold_daily_revenue CASCADE;
CREATE TABLE gold_daily_revenue AS
SELECT order_date AS order_day,
       category,
       COUNT(*) AS total_orders,
       COUNT(DISTINCT customer_id) AS unique_customers,
       SUM(total_amount) AS daily_revenue,
       ROUND(AVG(total_amount)::numeric, 2) AS avg_order_value
FROM silver_transactions 
GROUP BY 1, 2
ORDER BY order_day DESC, daily_revenue DESC;

-- 2. RFM SIMPLIFIÉ (La récence est maintenant basée sur la date d'achat réelle)
-- 2. RFM SIMPLIFIÉ (Correction du calcul de récence)
DROP TABLE IF EXISTS gold_customer_rfm CASCADE;
CREATE TABLE gold_customer_rfm AS
WITH rfm_base AS (
    SELECT 
        customer_id,
        COUNT(*) AS frequency,
        SUM(total_amount) AS monetary_value,
        -- En PostgreSQL, soustraire deux dates donne directement un INTEGER
        (CURRENT_DATE - MAX(order_date)) AS recency_days, -- <-- MODIFICATION ICI
        COUNT(DISTINCT category) AS category_diversity
    FROM silver_transactions 
    GROUP BY customer_id
)
SELECT *,
    CASE 
        WHEN frequency >= 5 AND monetary_value >= 500 THEN 'VIP'
        WHEN frequency >= 3 OR monetary_value >= 300 THEN 'Loyal' 
        WHEN recency_days <= 7 THEN 'Recent'
        ELSE 'Regular'
    END AS customer_segment
FROM rfm_base;

-- 3. VÉRIFICATION
SELECT 
    ' GOLD TERMINÉ' AS status,
    (SELECT COUNT(*) FROM gold_daily_revenue) AS revenue_rows,
    (SELECT COUNT(*) FROM gold_customer_rfm) AS rfm_rows,
    (SELECT COUNT(*) FILTER (WHERE customer_segment = 'VIP') FROM gold_customer_rfm) AS vip_count,
    (SELECT ROUND(SUM(daily_revenue)::numeric, 0) FROM gold_daily_revenue) AS total_revenue;
"""

cur.execute("DROP VIEW IF EXISTS dim_category CASCADE;")
cur.execute("CREATE VIEW dim_category AS SELECT DISTINCT category FROM silver_transactions;")
conn.commit()
print(" Vue dim_category créée avec succès.")

cur.execute(sql_gold)
result = cur.fetchall()
conn.commit()

print(" RÉSULTATS GOLD :")
for row in result:
    print(row)

cur.close()

conn.close()
