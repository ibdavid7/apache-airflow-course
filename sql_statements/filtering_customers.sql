SELECT name, product, price
FROM complete_customer_details
WHERE price BETWEEN %(lower_bound)s AND %(upper_bound)s