\c postgres;

CREATE TABLE IF NOT EXISTS products (
  product_id   SERIAL PRIMARY KEY,
  name         VARCHAR(100) NOT NULL,
  category     VARCHAR(50)  NOT NULL,
  base_price   DECIMAL(10,2) NOT NULL,
  stock        INTEGER DEFAULT 0,
  active       BOOLEAN DEFAULT TRUE,
  created_at   TIMESTAMP DEFAULT NOW()
);

INSERT INTO products (name, category, base_price, stock) VALUES
  ('laptop',       'electronics',   1299.99, 80),
  ('phone',        'electronics',    899.99, 250),
  ('tablet',       'electronics',    649.99, 120),
  ('headphones',   'audio',          199.99, 300),
  ('keyboard',     'peripherals',     89.99, 500),
  ('monitor',      'peripherals',    449.99, 60),
  ('mouse',        'peripherals',     49.99, 700),
  ('webcam',       'peripherals',    129.99, 200),
  ('speaker',      'audio',          249.99, 150),
  ('smartwatch',   'wearables',      399.99, 100)
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS customers (
  user_id     INTEGER PRIMARY KEY,
  email       VARCHAR(150),
  tier        VARCHAR(20) DEFAULT 'bronze',   
  country     VARCHAR(50) DEFAULT 'US',
  joined_at   TIMESTAMP DEFAULT NOW()
);

INSERT INTO customers (user_id, email, tier, country)
SELECT
  g,
  'user' || g || '@example.com',
  CASE WHEN g % 10 = 0 THEN 'gold'
       WHEN g % 5 = 0  THEN 'silver'
       ELSE 'bronze' END,
  (ARRAY['US','UK','IN','DE','AU'])[1 + (g % 5)]
FROM generate_series(1000, 1999) g
ON CONFLICT DO NOTHING;


CREATE OR REPLACE VIEW product_summary AS
SELECT
  name,
  category,
  base_price,
  stock,
  CASE WHEN stock > 100 THEN 'in_stock'
       WHEN stock > 0   THEN 'low_stock'
       ELSE 'out_of_stock' END AS availability
FROM products;
