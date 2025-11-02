SELECT name, product, price
FROM complete_customer_details
WHERE name = ANY(%(names)s);