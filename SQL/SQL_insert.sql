INSERT INTO customer (customer_name, customer_surname, customer_age)
SELECT (ARRAY['leon','Claire','Chris','Bryan','Jill','Ada'])[1 + floor(random() * 6)::int],
       (ARRAY['Kenendy','Redfield','Valentine','Smith','White'])[1 + floor(random() * 5)::int],
       18 + floor(random() * 60)::int
FROM generate_series(1, 5);