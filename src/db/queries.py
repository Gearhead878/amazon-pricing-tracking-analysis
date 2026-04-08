sql_price_imports = """
INSERT INTO price_imports(price_month, source_hash, row_count) 
VALUES (%s, %s, %s)
"""

sql_price_temp_table = """
CREATE TEMPORARY TABLE update_product_price(
asin char(10) not null primary key,
model char(250) not null,
price decimal(10, 2) not null
)
"""

sql_price_written = """
INSERT INTO update_product_price(asin, model, price) VALUES (%s, %s, %s)
"""

sql_asin_update = """
INSERT INTO asins (asin)
SELECT up.asin
FROM update_product_price AS up
LEFT JOIN asins a ON a.asin = up.asin
WHERE a.asin IS NULL;
"""

sql_products_update = """
INSERT INTO products (asin_id, model)
SELECT new.asin_id, new.model
FROM (
  SELECT a.id AS asin_id, up.model AS model
  FROM update_product_price AS up
  JOIN asins a ON a.asin = up.asin
) AS new
ON DUPLICATE KEY UPDATE
  products.model = new.model;
"""

sql_price_updates = """
INSERT INTO prices (asin_id, price, price_month, imported_id)
SELECT new.asin_id, new.price, new.price_month, new.imported_id
FROM (
  SELECT a.id AS asin_id, up.price AS price, %s AS price_month, %s AS imported_id
  FROM update_product_price AS up
  JOIN asins a ON a.asin = up.asin
) AS new
ON DUPLICATE KEY UPDATE
  prices.price = new.price,
  prices.imported_id = new.imported_id;
"""

sql_eol_update = """
UPDATE products p
LEFT JOIN prices pr
  ON pr.asin_id = p.asin_id
 AND pr.imported_id = %s
SET p.eol_status = CASE
  WHEN pr.id IS NULL THEN 1
  ELSE 0
END;
"""

sql_fetch_asins = """
SELECT a.id AS asin_id,
a.asin AS asin
FROM asins AS a
INNER JOIN prices AS pr
    ON a.id = pr.asin_id
WHERE pr.imported_id = (
    SELECT MAX(p2.imported_id)
    FROM prices AS p2
    WHERE p2.price_month <= %s
);
"""

sql_fetch_asins_manual_base = """
SELECT a.id AS asin_id,
asin
FROM asins AS a
WHERE asin IN ({placeholders})
"""

sql_reseller_written = """
INSERT INTO resellers(name)
VALUES (%(name)s)
ON DUPLICATE KEY UPDATE name = name;
"""

sql_amz_product_snapshots_written = """
INSERT INTO amz_product_snapshots(asin_id, snapshot_time, snapshot_date, title, asin_suppressed, is_carried)
VALUES (%(asin_id)s, %(snapshot_time)s, %(snapshot_date)s, %(title)s, %(asin_suppressed)s, %(is_carried)s);
"""

sql_insert_amz_reseller_prices = """
INSERT INTO amz_reseller_prices (
    snapshot_id,
    reseller_id,
    offer_no,
    price,
    is_buybox
)
VALUES (
    %(snapshot_id)s,
    %(reseller_id)s,
    %(offer_no)s,
    %(price)s,
    %(is_buybox)s
) AS new
ON DUPLICATE KEY UPDATE
    price = new.price,
    is_buybox = new.is_buybox
"""

sql_select_snapshot_ids = """
SELECT id, asin_id
FROM amz_product_snapshots
WHERE snapshot_time = %s
"""

sql_select_resellers_base = """
SELECT id, name
FROM resellers
WHERE name IN ({placeholders});
"""

sql_export_latest_prices_by_date = """
WITH latest_valid_snapshots AS (
    SELECT
        aps.id AS snapshot_id,
        aps.asin_id,
        aps.snapshot_time,
        aps.snapshot_date,
        ROW_NUMBER() OVER (
            PARTITION BY aps.asin_id
            ORDER BY aps.snapshot_time DESC, aps.id DESC
        ) AS rn
    FROM amz_product_snapshots AS aps
    WHERE aps.snapshot_date = %s
      AND aps.title IS NOT NULL
      AND TRIM(aps.title) <> ''
),
latest_srp AS (
    SELECT
        p2.asin_id,
        p2.price AS srp,
        ROW_NUMBER() OVER (
            PARTITION BY p2.asin_id
            ORDER BY p2.price_month DESC, p2.id DESC
        ) AS rn
    FROM prices AS p2
)
SELECT
    a.asin AS ASIN,
    r.name AS Reseller,
    pr.model AS ModelName,
    lsrp.srp AS SRP,
    arp.price AS CurPrice,
    NULL AS Note
FROM latest_valid_snapshots AS lvs
INNER JOIN asins AS a
    ON a.id = lvs.asin_id
INNER JOIN products AS pr
    ON pr.asin_id = a.id
INNER JOIN amz_reseller_prices AS arp
    ON arp.snapshot_id = lvs.snapshot_id
INNER JOIN resellers AS r
    ON r.id = arp.reseller_id
LEFT JOIN latest_srp AS lsrp
    ON lsrp.asin_id = a.id
   AND lsrp.rn = 1
WHERE lvs.rn = 1
ORDER BY a.asin, r.name, arp.offer_no
"""

sql_check_price_table_date = """
SELECT max(price_month) AS price_month
FROM prices
LIMIT 1;
"""

sql_export_asin_suppressed = """
WITH latest_snapshots AS (
    SELECT
        aps.asin_id,
        aps.asin_suppressed,
        ROW_NUMBER() OVER (
            PARTITION BY aps.asin_id
            ORDER BY aps.snapshot_time DESC
        ) AS rn
    FROM amz_product_snapshots aps
    WHERE aps.snapshot_date = %s
)
SELECT a.asin
FROM latest_snapshots ls
JOIN asins a ON a.id = ls.asin_id
WHERE ls.rn = 1
  AND ls.asin_suppressed = 1;
"""