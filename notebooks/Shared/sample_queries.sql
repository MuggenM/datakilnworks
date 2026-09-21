-- Shared Lakehouse Analytical Queries
-- Query: Customer Lifetime Value & Order Distribution

SELECT 
    c.customer_id,
    c.full_name,
    COUNT(o.order_id) AS total_orders,
    ROUND(SUM(o.amount), 2) AS total_spent,
    ROUND(AVG(o.amount), 2) AS avg_order_value,
    MAX(o.order_date) AS last_order_date
FROM dbo.dim_customers c
LEFT JOIN dbo.bronze_orders o ON c.customer_id = o.customer_id
WHERE o.status = 'COMPLETED'
GROUP BY c.customer_id, c.full_name
ORDER BY total_spent DESC
LIMIT 10;
