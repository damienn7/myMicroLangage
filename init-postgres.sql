
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    age INT
);

INSERT INTO users (name, age) VALUES
('Alice', 30),
('Bob', 25),
('Charlie', 35);
