use source
SELECT TOP 10 * FROM [core].[clients]
ORDER BY [id] DESC


INSERT INTO [core].[clients] (fullname, preferred_store_id, registration_date, churned)
values('abcd', 1, '2025-01-01', 1)

DELETE FROM [core].[clients]
WHERE id = 4180

CREATE TABLE [core].[order_product]
(
order_id INT NOT NULL,
product_id INT NOT NULL,
quantity decimal(10,2),
FOREIGN KEY (order_id) REFERENCES [core].[orders](id),
FOREIGN KEY (product_id) REFERENCES [core].[products](id)
)


SELECT * FROM [core].[orders]

ALTER TABLE [core].[products]
ADD CONSTRAINT pk_products PRIMARY KEY (id);

ALTER TABLE [core].[orders]
DROP CONSTRAINT FK__orders__client_i__75A278F5;


ALTER TABLE [core].[products]
ALTER COLUMN [id] INT NOT NULL


CREATE TABLE [core].[orders]
(
id INT NOT NULL PRIMARY KEY,
client_id INT NOT NULL,
order_date date,
order_source_id INT NOT NULL,
total_amount MONEY,
-- FOREIGN KEY (client_id) REFERENCES [core].[client_id](id)
)

ALTER TABLE [core].[orders]
ADD CONSTRAINT pk_orders PRIMARY KEY (id);

TRUNCATE TABLE [core].[orders]
TRUNCATE TABLE [core].[order_product]

SELECT count(*) FROM [core].[orders]
SELECT * FROM [core].[order_product]
SELECT * FROM [core].[order_statuses]

INSERT INTO [core].[order_statuses]
VALUES(1, 'Created'), (2, 'Packing'), (3, 'Delivered'), (4, 'Cancelled')

TRUNCATE TABLE [core].[orders]
TRUNCATE TABLE [core].[delivery_tracking]
TRUNCATE TABLE [core].[order_status_history]


SELECT count(*) FROM [core].[orders]
UNION ALL
SELECT count(*) FROM [core].[delivery_tracking]
UNION ALL
SELECT count(*) FROM [core].[order_status_history]

CREATE TABLE [core].[order_status_history]
(
order_id INT NOT NULL,
order_status_id INT NOT NULL,
status_datetime datetime2(2)
)


CREATE TABLE [core].[delivery_tracking]
(
order_id INT NOT NULL,
courier_id INT NOT NULL,
status VARCHAR(50),
status_time TIMESTAMP
)


SELECT *  FROM [core].[order_status_history]
WHERE order_id = 47


with st1 as
(SELECT * FROM [core].[order_status_history]
where order_status_id = 1),

st2 as
(SELECT * FROM [core].[order_status_history]
where order_status_id = 2),

st3 as
(SELECT * FROM [core].[order_status_history]
where order_status_id = 3)

SELECT s3.order_id, s3.status_datetime as "del_time", s2.status_datetime as "pack_time", s1.status_datetime as "create_time" FROM st3 s3
LEFT JOIN st2 s2 ON s2.order_id = s3.order_id
LEFT JOIN st1 s1 ON s3.order_id = s1.order_id
WHERE s3.status_datetime < s2.status_datetime OR s3.status_datetime < s1.status_datetime