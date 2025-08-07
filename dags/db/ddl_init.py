from generators.config import schema_init

schema = schema_init

clients_query = f"""
CREATE TABLE [{schema}].[clients](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[fullname] [varchar](255) NOT NULL,
	[preferred_store_id] [tinyint] NULL,
	[registration_date] [date] NULL,
	[churned] [bit] NULL,
)
"""
stores_query = f"""
CREATE TABLE [{schema}].[stores](
	[id] [tinyint] NOT NULL,
	[city] [varchar](50) NOT NULL,
	[store_name] [varchar](50) NOT NULL
)
"""

zones_query = f"""
CREATE TABLE [{schema}].[zones](
	[id] [tinyint] NOT NULL,
	[store_id] [tinyint] NOT NULL,
	[zone_name] [varchar](50) NOT NULL
)
"""

assortment_query = f"""
CREATE TABLE [{schema}].[assortment](
	[product_id] [smallint] NOT NULL,
	[store_id] [tinyint] NOT NULL
)
"""

costing_query = f"""
CREATE TABLE [{schema}].[costing](
	[product_id] [smallint] NOT NULL,
	[store_id] [tinyint] NOT NULL,
	[avg_cost] [decimal](10,2) NOT NULL,
	[cost_active_period] [date] NOT NULL
)
"""

products_query = f"""
CREATE TABLE [{schema}].[products](
	[id] [int] NOT NULL,
	[product_name] [varchar](255) NOT NULL, 
	[price] [decimal](10,2) NOT NULL,
	[unit_type] [varchar](50) NOT NULL, 
	[category_id] [smallint] NOT NULL,
	[chance] [decimal](4, 2) NOT NULL,
)
"""

product_categories_query = f"""
CREATE TABLE [{schema}].[product_categories](
	[id] [smallint] NOT NULL,
	[category_name] [varchar](50) NOT NULL
)
"""

payments_query = f"""
CREATE TABLE [{schema}].[payments](
	[order_id] [int] NOT NULL,
	[amount] [decimal](12, 4) NULL,
	[method] [varchar](50),
	[payment_status] [varchar](50),
	[payment_date] [date],
)
"""

delivery_tracking_query = f"""
CREATE TABLE [{schema}].[delivery_tracking](
	[order_id] [int] NOT NULL,
	[courier_id] [smallint] NOT NULL,
	[delivery_status_id] [tinyint] NULL,
	[status] [varchar](50),
	[status_datetime] [datetime2](2) NULL
)
"""

delivery_resource_query = f"""
CREATE TABLE [{schema}].[delivery_resource](
	[id] [smallint] NOT NULL,
	[fullname] [varchar](50) NOT NULL,
	[delivery_type_id] [tinyint] NOT NULL,
	[store_id] [tinyint] NOT NULL,
	[active_flag] [bit] NOT NULL
)
"""

delivery_type_query = f"""
CREATE TABLE [{schema}].[delivery_type](
	[id] [tinyint] NOT NULL,
	[courier_type] [varchar](50) NOT NULL
)
"""

delivery_costing_query = f"""
CREATE TABLE [{schema}].[delivery_costing](
	[zone_id] [tinyint] NOT NULL,
	[delivery_type_id] [tinyint] NOT NULL,
	[tariff] [decimal](4,2) NOT NULL
)
"""

orders_query = f"""
CREATE TABLE [{schema}].[orders](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[client_id] [int] NULL,
	[order_date] [date] NULL,
	[order_source_id] [tinyint] NULL,
	[total_amount] [decimal](12,4) NULL,
)
"""

order_product_query = f"""
CREATE TABLE [{schema}].[order_product](
	[order_id] [int] NOT NULL,
	[product_id] [int] NOT NULL,
	[quantity] [decimal](6, 2) NULL
)
"""

order_status_history_query = f"""
CREATE TABLE [{schema}].[order_status_history](
	[order_id] [int] NOT NULL,
	[order_status_id] [tinyint] NOT NULL,
	[status_datetime] [datetime2](2) NOT NULL
)
"""

order_statuses_query = f"""
CREATE TABLE [{schema}].[order_statuses](
	[id] [tinyint] NOT NULL,
	[order_status] [varchar](50) NOT NULL
)
"""

table_queries = {
    "clients": {
        "query": clients_query,
        "type": "init",
    },
    "stores": {
        "query": stores_query,
        "type": "dict",
    },
    "zones": {
        "query": zones_query,
        "type": "dict",
    },
    "assortment": {
        "query": assortment_query,
        "type": "init",
    },
    "costing": {
        "query": costing_query,
        "type": "dict",
    },
    "products": {
        "query": products_query,
        "type": "dict",
    },
    "product_categories": {
        "query": product_categories_query,
        "type": "dict",
    },
    "payments": {
        "query": payments_query,
        "type": "gen",
    },
    "delivery_tracking": {
        "query": delivery_tracking_query,
        "type": "gen",
    },
    "delivery_resource": {
        "query": delivery_resource_query,
        "type": "init",
    },
    "delivery_type": {
        "query": delivery_type_query,
        "type": "dict",
    },
    "delivery_costing": {
        "query": delivery_costing_query,
        "type": "dict",
    },
    "orders": {
        "query": orders_query,
        "type": "gen",
    },
    "order_product": {
        "query": order_product_query,
        "type": "gen",
    },
    "order_status_history": {
        "query": order_status_history_query,
        "type": "gen",
    },
    "order_statuses": {
        "query": order_statuses_query,
        "type": "dict",
    },
}
