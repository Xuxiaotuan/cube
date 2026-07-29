-- 测试数据：电商订单表
CREATE TABLE IF NOT EXISTS public.orders (
    id SERIAL PRIMARY KEY,
    status TEXT NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

INSERT INTO public.orders (status, amount, created_at) VALUES
    ('completed', 150.00, '2026-01-15 10:30:00'),
    ('completed', 230.00, '2026-01-20 14:00:00'),
    ('processing', 89.99, '2026-02-03 09:15:00'),
    ('completed', 420.50, '2026-02-10 16:45:00'),
    ('shipped', 175.00, '2026-02-18 11:20:00'),
    ('cancelled', 60.00, '2026-03-01 08:00:00'),
    ('completed', 310.00, '2026-03-05 13:30:00'),
    ('processing', 95.00, '2026-03-12 10:00:00'),
    ('shipped', 550.00, '2026-03-20 15:10:00'),
    ('completed', 180.00, '2026-04-01 09:00:00'),
    ('completed', 275.00, '2026-04-08 12:30:00'),
    ('processing', 120.00, '2026-04-15 14:00:00'),
    ('shipped', 390.00, '2026-04-22 11:00:00'),
    ('completed', 200.00, '2026-05-01 10:00:00'),
    ('cancelled', 45.00, '2026-05-05 16:30:00'),
    ('completed', 610.00, '2026-05-12 09:20:00'),
    ('shipped', 165.00, '2026-05-18 13:45:00'),
    ('processing', 280.00, '2026-06-01 08:30:00'),
    ('completed', 350.00, '2026-06-10 15:00:00'),
    ('shipped', 420.00, '2026-06-20 10:15:00');
