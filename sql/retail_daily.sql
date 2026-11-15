SELECT
    day,
    country,
    count(*) AS accepted_lines,
    count(*) FILTER (WHERE quantity > 0) AS sales_lines,
    count(*) FILTER (WHERE quantity < 0) AS negative_quantity_lines,
    count(*) FILTER (WHERE is_cancellation) AS cancellation_lines,
    count(*) FILTER (WHERE unit_price = 0) AS zero_price_lines,
    count(*) FILTER (WHERE country = 'United Kingdom') AS uk_lines,
    count(*) FILTER (WHERE country = 'United Kingdom' AND has_customer_id) AS uk_lines_with_id,
    coalesce(sum(line_amount) FILTER (WHERE line_amount > 0), 0) AS gross_sales_value,
    coalesce(-sum(line_amount) FILTER (WHERE line_amount < 0), 0) AS negative_line_value,
    sum(line_amount) AS net_line_value
FROM retail_lines
GROUP BY day, country
ORDER BY day, country;
