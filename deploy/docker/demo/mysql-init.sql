-- The replication user and a table with a few rows, which binwave loads before it follows the binlog.
CREATE USER 'binwave'@'%' IDENTIFIED BY 'demo';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'binwave'@'%';

CREATE DATABASE shop;
CREATE TABLE shop.items (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  price DECIMAL(10, 2) NOT NULL,
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
);
INSERT INTO shop.items (name, price) VALUES ('pen', 1.50), ('notebook', 4.20), ('lamp', 19.99);
