CREATE TABLE [core].[orders]
(
id INT NOT NULL PRIMARY KEY,
client_id INT NOT NULL,
order_date date,
order_source_id INT NOT NULL,
total_amount MONEY,
-- FOREIGN KEY (client_id) REFERENCES [core].[client_id](id)
)

CREATE TABLE [core].[order_product]
(
order_id INT NOT NULL,
product_id INT NOT NULL,
quantity decimal(10,2),
-- FOREIGN KEY (order_id) REFERENCES [core].[orders](id),
-- FOREIGN KEY (product_id) REFERENCES [core].[products](id)
)

CREATE TABLE [core].[delivery_tracking]
(
order_id INT NOT NULL,
courier_id INT NOT NULL,
delivery_status_id TINYINT NOT NULL,
status VARCHAR(50),
status_time datetime2(2)
)