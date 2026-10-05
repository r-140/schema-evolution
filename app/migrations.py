from .storage import pg, catalog
from psycopg import sql

MIGRATIONS = {
 'add-channel': 'ALTER TABLE sales.orders ADD COLUMN delivery_channel TEXT',
 'change-pk': 'ALTER TABLE sales.orders DROP CONSTRAINT orders_pkey; ALTER TABLE sales.orders ADD PRIMARY KEY(id,customer_id)',
 'add-country': "ALTER TABLE sales.orders ADD COLUMN country TEXT",
 'rename-amount': "ALTER TABLE sales.orders RENAME COLUMN amount TO total_amount",
 'amount-text': "ALTER TABLE sales.orders ALTER COLUMN amount TYPE TEXT USING amount::text",
 'expand-amount': "ALTER TABLE sales.orders ADD COLUMN total_amount BIGINT; UPDATE sales.orders SET total_amount=amount; ALTER TABLE sales.orders ALTER COLUMN total_amount SET NOT NULL",
 'contract-amount': "ALTER TABLE sales.orders DROP COLUMN amount; ALTER TABLE sales.orders ALTER COLUMN total_amount SET NOT NULL",
 'nullable-amount': "ALTER TABLE sales.orders ALTER COLUMN amount DROP NOT NULL",
}

def migrate(name):
    with pg() as c:
        with c.transaction():
            c.execute(MIGRATIONS[name])
    return catalog()

def populate_country(limit):
    # DDL addition cannot reconstruct old business values. Explicit demo source enrichment.
    with pg() as c:
        return c.execute("UPDATE sales.orders SET country=CASE id %% 3 WHEN 0 THEN 'PL' WHEN 1 THEN 'DE' ELSE 'FR' END,updated_at=now() WHERE id IN (SELECT id FROM sales.orders WHERE country IS NULL ORDER BY id LIMIT %s)",(limit,)).rowcount
