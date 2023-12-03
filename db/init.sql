GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

CREATE TABLE statistic (
    id SERIAL PRIMARY KEY,
    exchange varchar(20),
    transaction_date timestamp,
    transaction_type varchar(10),
    amount decimal(38, 8),
    price decimal(38, 8)
);

