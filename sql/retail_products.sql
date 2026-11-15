SELECT
    day,
    country,
    stock_code,
    count(*) FILTER (WHERE quantity > 0) AS sales_lines,
    count(*) FILTER (WHERE quantity < 0) AS negative_quantity_lines,
    coalesce(sum(line_amount) FILTER (WHERE line_amount > 0), 0) AS gross_sales_value,
    coalesce(-sum(line_amount) FILTER (WHERE line_amount < 0), 0) AS negative_line_value,
    sum(line_amount) AS net_line_value
FROM retail_lines
GROUP BY day, country, stock_code
ORDER BY day, country, stock_code;
