CREATE TABLE customer (
    customer_id serial primary key,
    customer_name varchar not null,
    customer_surname varchar not null,
    customer_age int not null,
    updated_at timestamptz not null default now()
);