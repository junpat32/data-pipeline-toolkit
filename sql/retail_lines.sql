CREATE TEMP VIEW retail_lines AS
SELECT
    CAST(invoice_date AS DATE) AS day,
    country,
    stock_code,
    quantity,
    unit_price,
    customer_id IS NOT NULL AS has_customer_id,
    lower(left(invoice_no, 1)) = 'c' AS is_cancellation,
    CAST(quantity AS DECIMAL(19, 0)) * unit_price AS line_amount
FROM rows_retail;
