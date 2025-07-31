IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'core')
	EXEC('CREATE SCHEMA core');

BEGIN TRANSACTION
    DROP TABLE IF EXISTS core.clients

    SELECT * 
    INTO core.clients
    FROM stage.clients

COMMIT


INSERT INTO core.clients (fullname, preferred_store_id, registration_date, churned)
SELECT fullname, preferred_store_id, registration_date, churned 
FROM stage.clients_init

CREATE TABLE core.clients (
    id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
	fullname NVARCHAR(50) NOT NULL,
    preferred_store_id INT,
    registration_date date,
	churned bit
);


SELECT 
	[preferred_store_id], 
	COUNT(*), 
	COUNT(CASE WHEN churned = 1 THEN 1 END) AS churned_clients, 
	count(*) - COUNT(CASE WHEN churned = 1 THEN 1 END) as ttl 
FROM [core].[clients]
GROUP BY [preferred_store_id]
UNION ALL
SELECT 
	NULL as [preferred_store_id] , 
	count(*), 
	COUNT(CASE WHEN churned = 1 THEN 1 END) AS churned_clients, 
	count(*) - COUNT(CASE WHEN churned = 1 THEN 1 END) as ttl 
FROM [core].[clients]



SELECT 
	[preferred_store_id], 
	COUNT(*), 
	COUNT(CASE WHEN churned = 1 THEN 1 END) AS churned_clients, 
	count(*) - COUNT(CASE WHEN churned = 1 THEN 1 END) as ttl 
FROM [stage].[clients_init]
GROUP BY [preferred_store_id]
UNION ALL
SELECT 
	NULL as [preferred_store_id] , 
	count(*), 
	COUNT(CASE WHEN churned = 1 THEN 1 END) AS churned_clients, 
	count(*) - COUNT(CASE WHEN churned = 1 THEN 1 END) as ttl 
FROM [stage].[clients_init]


DECLARE @sql NVARCHAR(MAX) = '';

SELECT @sql = @sql + 'DROP TABLE [' + s.name + '].[' + t.name + '];'
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = 'stage'  -- ← замени на свою схему

-- Отключить ограничения, если нужно
-- EXEC sp_msforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL'

EXEC sp_executesql @sql;
