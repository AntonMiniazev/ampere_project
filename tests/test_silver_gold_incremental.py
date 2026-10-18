"""Run v2 domain and dataset fixtures locally without changing upstream data."""

from __future__ import annotations

import os
import io
import json
import sys
import tempfile
import unittest
import uuid
from contextlib import contextmanager, redirect_stdout
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
from jinja2 import Environment, StrictUndefined

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'docker/dbt_iceberg'))
import incremental_state  # noqa: E402
from query_metrics import execute_measured  # noqa: E402
from tools.contracts.ampere_contract import load_contract  # noqa: E402
from validate_staged import compare_table  # noqa: E402


def render(model: str, **settings) -> str:
    values = {'ICEBERG_DATASET_ID': 'active', 'SILVER_DELTA_FROM_SEQUENCE': '1',
              'SILVER_DELTA_TO_SEQUENCE': '3', 'GOLD_AFFECTED_MONTHS': '*'} | settings
    values['GOLD_RUN_MODE'] = 'full_history' if values['GOLD_AFFECTED_MONTHS'] == '*' else 'daily_refresh'
    env = Environment(undefined=StrictUndefined)
    env.globals.update(
        config=lambda **kwargs: '', ref=lambda name: f'"{name}"',
        source=lambda schema, name: f'"{schema}"."{name}"',
        env_var=lambda key, default=None: values.get(key, default),
        var=lambda key, default=None: default,
        ampere_silver_run_mode=lambda: 'daily_refresh',
        ampere_silver_lineage_columns=lambda: "TIMESTAMP '2026-10-01' AS _silver_build_ts, 'fixture' AS _silver_model_name, 'fixture' AS _silver_run_id",
    )
    macros = '\n'.join((ROOT / 'dbt_iceberg/macros' / name).read_text()
                       for name in ('raw_delta.sql', 'gold_months.sql'))
    return env.from_string(macros + (ROOT / 'dbt_iceberg/models' / model).read_text(encoding='utf-8-sig')).render()


class IncrementalSQLTests(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect()

    def tearDown(self):
        self.con.close()

    def test_scan_metrics_come_from_executed_query(self):
        with tempfile.TemporaryDirectory() as directory:
            path = (Path(directory) / 'input.parquet').as_posix()
            self.con.execute(f"COPY (SELECT * FROM range(10)) TO '{path}' (FORMAT PARQUET)")
            output = io.StringIO()
            with patch.dict(os.environ, {'ICEBERG_SCAN_PROFILING': 'true', 'DUCKDB_PATH': str(Path(directory) / 'work.duckdb')}), redirect_stdout(output):
                rows = execute_measured(self.con, f"SELECT * FROM read_parquet('{path}') WHERE range > 3", 'fixture')
            self.assertEqual(len(rows), 6)
            metrics = json.loads(output.getvalue())
            self.assertGreater(metrics['rows_scanned'], 0)
            self.assertTrue(metrics['file_scan_metrics_available'])

    def test_read_only_full_reference_detects_missing_moved_and_duplicate_rows(self):
        for table in ('reference', 'published'):
            self.con.execute(f'CREATE TABLE {table}(month DATE,store_id INTEGER,product_id INTEGER,amount INTEGER)')
        self.con.execute("INSERT INTO reference VALUES ('2026-02-01',1,1,20)")
        self.con.execute("INSERT INTO published VALUES ('2026-01-01',1,1,20),('2026-01-01',1,1,20)")
        before = self.con.execute('SELECT * FROM published').fetchall()
        result = compare_table(self.con, 'reference', 'published', ['month','store_id','product_id','amount'])
        self.assertEqual((result['missing_rows'],result['extra_rows']), (1,2))
        self.assertEqual(self.con.execute('SELECT * FROM published').fetchall(), before)

    def test_budget_reader_preserves_invalid_rows_for_validation_and_allows_empty_file(self):
        header = 'budget_name,budget_date,store_id,orders_budget_daily,sales_amount_daily\n'
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'budget.csv'
            path.write_text(header + ',2026-01-01,1,1,10\n')
            sql = render('marts/silver_budget_orders_sales.sql', BUDGET_DAILY_CSV_PATH=path.as_posix())
            rows = self.con.execute(sql).fetchall()
            self.assertEqual(len(rows), 1)
            self.assertIsNone(rows[0][0])
            path.write_text(header)
            self.assertEqual(self.con.execute(sql).fetchall(), [])

    def test_every_raw_delta_branch_is_dataset_and_sequence_scoped(self):
        self.con.execute('CREATE SCHEMA raw; CREATE SCHEMA bronze')
        raw = {
            'orders': 'id INTEGER', 'order_product': 'order_id INTEGER',
            'payments': 'order_id INTEGER', 'order_status_history': 'order_id INTEGER',
            'delivery_tracking': 'order_id INTEGER', 'clients': 'id INTEGER',
            'stores': 'id INTEGER', 'zones': 'id INTEGER,store_id INTEGER',
            'products': 'id INTEGER', 'costing': 'product_id INTEGER,store_id INTEGER',
            'product_categories': 'id INTEGER', 'delivery_resource': 'id INTEGER',
            'order_statuses': 'id INTEGER',
            'delivery_type': 'id INTEGER', 'delivery_costing': 'zone_id INTEGER',
        }
        for table, columns in raw.items():
            self.con.execute(f'CREATE TABLE raw.{table} ({columns},_dataset_id VARCHAR,_batch_sequence BIGINT)')
            if table != 'orders':
                fields = columns.count(',') + 1
                self.con.execute(f"INSERT INTO raw.{table} VALUES ({','.join(['1'] * fields)},'retired',2)")
        self.con.execute("INSERT INTO raw.orders VALUES (101,'active',2),(1,'retired',2),(404,'active',4),(100,'active',1)")
        for table, columns, rows in (
            ('orders', 'id INTEGER,client_id INTEGER,store_id INTEGER,order_zone_id INTEGER', '(1,1,1,1)'),
            ('order_product', 'order_id INTEGER,product_id INTEGER', '(1,1)'),
            ('payments', 'order_id INTEGER', '(1)'),
            ('order_status_history', 'order_id INTEGER,order_status_id INTEGER', '(1,1)'),
            ('products', 'id INTEGER,category_id INTEGER', '(1,1)'),
            ('zones', 'id INTEGER,store_id INTEGER', '(1,1)'),
            ('delivery_tracking', 'order_id INTEGER,courier_id INTEGER', '(1,1)'),
            ('delivery_resource', 'id INTEGER,delivery_type_id INTEGER', '(1,1)'),
        ):
            self.con.execute(f'CREATE TABLE bronze.{table} ({columns}); INSERT INTO bronze.{table} VALUES {rows}')
        sql = render('intermediate/affected_order_ids.sql')
        self.assertEqual([r[0] for r in self.con.execute(sql).fetchall()], [101])
        self.con.execute("INSERT INTO raw.product_categories VALUES (1,'active',2)")
        self.assertEqual({r[0] for r in self.con.execute(sql).fetchall()}, {101, 1})
        self.con.execute('DELETE FROM raw.product_categories')
        self.con.execute("INSERT INTO raw.clients VALUES (1,'active',2)")
        scoped = {r[0]: r[1:] for r in self.con.execute(sql).fetchall()}
        self.assertEqual(scoped[1], (False, False, False, False, False))
        self.assertEqual(scoped[101], (True, True, True, True, True))
        self.con.execute("INSERT INTO raw.payments VALUES (1,'active',2)")
        scoped = {r[0]: r[1:] for r in self.con.execute(sql).fetchall()}
        self.assertEqual(scoped[1], (False, False, False, True, False))

    def test_preference_history_excludes_retired_and_future_records(self):
        self.con.execute('CREATE SCHEMA raw')
        self.con.execute('CREATE TABLE raw.clients (id INTEGER,preferred_store_id INTEGER,registration_date DATE,updated_at DATE,churned BOOLEAN,_business_date DATE,_batch_sequence BIGINT,_change_id BIGINT,_op VARCHAR,_dataset_id VARCHAR)')
        self.con.execute("INSERT INTO raw.clients VALUES (1,1,'2026-01-01',NULL,false,'2026-01-01',2,1,'I','active'),(1,99,'2026-01-01',NULL,false,'2026-01-01',2,1,'I','retired'),(1,9,'2026-01-01',NULL,false,'2026-01-01',4,1,'U','active')")
        rows = self.con.execute(render('marts/dimensions/dim_client_preference_history.sql')).fetchall()
        self.assertEqual([(r[0], r[1]) for r in rows], [(1, 1)])

    def test_fact_orders_preserves_contract_column_order_and_unsupported_zone_null(self):
        self.con.execute("CREATE TABLE stg_orders(order_id INTEGER,order_date DATE,client_id INTEGER,store_id INTEGER,order_zone_id INTEGER,preferred_zone_id_at_order INTEGER,order_source_id INTEGER,total_amount DECIMAL(12,4)); INSERT INTO stg_orders VALUES (1,'2026-01-01',1,1,10,10,1,20)")
        self.con.execute('CREATE TABLE dim_clients(client_id INTEGER,client_full_name VARCHAR,is_churned BOOLEAN)')
        self.con.execute('CREATE TABLE dim_stores(store_id INTEGER,store_name VARCHAR)')
        self.con.execute('CREATE TABLE int_order_value_rollup(order_id INTEGER,ordered_quantity DECIMAL(38,2),line_sales_amount DECIMAL(14,4),line_count BIGINT)')
        self.con.execute('CREATE TABLE int_orders_latest_status(order_id INTEGER,latest_order_status_id INTEGER,latest_order_status VARCHAR,latest_status_datetime TIMESTAMP)')
        result = self.con.execute(render('marts/facts/fact_orders.sql'))
        self.assertEqual([c[0] for c in result.description], [c['name'] for c in load_contract().table('silver','fact_orders').columns])
        self.assertEqual(result.fetchone()[-2:], (10,None))

    def cost_fixture(self):
        self.con.execute("CREATE TABLE fact_orders(order_id INTEGER,order_date DATE,store_id INTEGER,order_zone_id INTEGER,latest_order_status_id INTEGER,total_amount DECIMAL(12,4),client_id INTEGER)")
        self.con.execute("INSERT INTO fact_orders VALUES (1,'2026-10-01',1,10,3,100,1),(2,'2026-10-01',1,11,3,200,1),(3,'2026-10-01',1,NULL,3,300,1),(4,'2026-10-01',2,10,3,400,1),(5,'2026-10-01',1,12,3,500,1),(6,'2026-09-01',1,10,3,50,1)")
        self.con.execute("CREATE TABLE fact_delivery_tracking(order_id INTEGER,courier_id INTEGER,status_datetime TIMESTAMP,delivery_status_id INTEGER)")
        self.con.execute("INSERT INTO fact_delivery_tracking SELECT order_id,1,order_date,2 FROM fact_orders; INSERT INTO fact_delivery_tracking VALUES (1,1,'2026-10-02',2)")
        self.con.execute("CREATE TABLE dim_zones(zone_id INTEGER,store_id INTEGER); INSERT INTO dim_zones VALUES (10,1),(11,1),(12,1)")
        self.con.execute("CREATE TABLE dim_delivery_resource(delivery_resource_id INTEGER,delivery_type_id INTEGER,delivery_resource_name VARCHAR,courier_type VARCHAR); INSERT INTO dim_delivery_resource VALUES (1,1,'Courier','bike')")
        self.con.execute("CREATE TABLE dim_delivery_costing(zone_id INTEGER,delivery_type_id INTEGER,tariff DECIMAL(10,2)); INSERT INTO dim_delivery_costing VALUES (10,1,5),(11,1,7),(12,1,8),(12,1,9)")
        self.con.execute("CREATE TABLE dim_stores(store_id INTEGER,store_name VARCHAR,city VARCHAR); INSERT INTO dim_stores VALUES (1,'A','City'),(2,'B','City')")

    def test_multi_zone_selected_store_unknown_history_and_ambiguous_tariffs(self):
        self.cost_fixture()
        result = self.con.execute(render('gold/intermediate/int_delivery_order_costs.sql')).fetchall()
        by_id = {r[0]: (r[-2], r[-1]) for r in result}
        self.assertEqual(len(by_id), len(result))
        self.assertEqual(by_id[1], ('ambiguous_tariff', None))
        self.assertEqual(by_id[2], ('ambiguous_tariff', None))
        self.assertEqual(by_id[3], ('ambiguous_tariff', None))
        self.assertEqual(by_id[4], ('missing_store_zones', None))
        self.assertEqual(by_id[5], ('ambiguous_tariff', None))

    def test_monthly_cost_models_match_full_reference_without_fanout(self):
        self.cost_fixture()
        self.con.execute("CREATE TABLE fact_order_product AS SELECT cast(order_id AS VARCHAR) AS fact_order_product_key,order_id,order_date,store_id,1 AS product_id,1::DECIMAL(10,2) AS quantity,total_amount AS line_sales_amount FROM fact_orders")
        self.con.execute("CREATE TABLE dim_costing(costing_key VARCHAR,product_id INTEGER,store_id INTEGER,valid_from DATE,valid_to DATE,avg_cost DECIMAL(10,2)); INSERT INTO dim_costing VALUES ('a',1,1,'2026-01-01',NULL,10),('b',1,2,'2026-01-01',NULL,20)")
        self.con.execute("CREATE TABLE dim_products(product_id INTEGER,product_name VARCHAR,category_name VARCHAR); INSERT INTO dim_products VALUES (1,'Product','Category')")
        self.con.execute("CREATE TABLE silver_budget_orders_sales(budget_date DATE,store_id INTEGER,sales_amount_daily DECIMAL(12,4))")
        for name in ('int_delivery_order_costs', 'int_product_line_costs'):
            self.con.execute(f'CREATE TABLE {name} AS ' + render(f'gold/intermediate/{name}.sql'))
        for model in ('financial_performance_monthly_store', 'financial_product_margin_monthly_store', 'delivery_courier_performance_monthly_store'):
            reference = [r for r in self.con.execute(render(f'gold/marts/{model}.sql')).fetchall() if r[0] == date(2026, 10, 1)]
            delta = self.con.execute(render(f'gold/marts/{model}.sql', GOLD_AFFECTED_MONTHS='2026-10-01')).fetchall()
            self.assertEqual(sorted(reference), sorted(delta))
        financial = self.con.execute(render('gold/marts/financial_performance_monthly_store.sql', GOLD_AFFECTED_MONTHS='2026-10-01')).fetchall()
        self.assertEqual(sum(r[4] for r in financial), 1500)
        self.assertTrue(all(r[6] is None and r[7] is None for r in financial))
        self.con.execute('UPDATE dim_costing SET avg_cost=11 WHERE store_id=1')
        self.con.execute('CREATE OR REPLACE TABLE int_product_line_costs AS ' + render('gold/intermediate/int_product_line_costs.sql'))
        corrected = self.con.execute(render('gold/marts/financial_product_margin_monthly_store.sql', GOLD_AFFECTED_MONTHS='2026-10-01')).fetchall()
        self.assertEqual(sum(r[-1] for r in corrected), 64)
        self.con.execute("INSERT INTO dim_costing VALUES ('overlap',1,1,'2026-01-01',NULL,30)")
        self.con.execute('CREATE OR REPLACE TABLE int_product_line_costs AS ' + render('gold/intermediate/int_product_line_costs.sql'))
        ambiguous = self.con.execute(render('gold/marts/financial_product_margin_monthly_store.sql', GOLD_AFFECTED_MONTHS='2026-10-01')).fetchall()
        self.assertEqual(sum(r[-2] for r in ambiguous), 1500)
        self.assertTrue(any(r[-1] is None for r in ambiguous))

    def test_preferred_store_intervals_use_month_start_and_end_and_unknown_history(self):
        self.con.execute("CREATE TABLE dim_client_preference_history(client_id INTEGER,preferred_store_id INTEGER,registration_date DATE,updated_at DATE,is_churned BOOLEAN,effective_from DATE,source_batch_sequence BIGINT,source_change_id BIGINT,is_deleted BOOLEAN)")
        self.con.execute("INSERT INTO dim_client_preference_history VALUES (1,1,'2025-12-01',NULL,false,'2026-01-01',1,1,false),(1,2,'2025-12-01','2026-02-15',false,'2026-02-15',2,1,false)")
        self.con.execute("CREATE TABLE fact_orders(order_date DATE,store_id INTEGER,client_id INTEGER,latest_order_status_id INTEGER); INSERT INTO fact_orders VALUES ('2025-12-01',1,1,3),('2026-01-01',1,1,3),('2026-02-01',1,1,3),('2026-03-01',2,1,3)")
        self.con.execute("CREATE TABLE dim_stores(store_id INTEGER,store_name VARCHAR,city VARCHAR); INSERT INTO dim_stores VALUES (1,'A','City'),(2,'B','City')")
        rows = self.con.execute(render('gold/marts/marketing_client_metrics_monthly_store.sql')).fetchall()
        by_key = {(r[0],r[1]): r for r in rows}
        self.assertIsNone(by_key[(date(2025,12,1),1)][6])
        self.assertEqual(by_key[(date(2026,2,1),1)][6], 1)
        self.assertEqual(by_key[(date(2026,3,1),2)][6], 1)
        daily = self.con.execute(render('gold/marts/marketing_client_metrics_monthly_store.sql', GOLD_AFFECTED_MONTHS='2026-02-01')).fetchall()
        self.assertEqual(sorted(daily), sorted(r for r in rows if r[0] == date(2026,2,1)))


class CheckpointTests(unittest.TestCase):
    @contextmanager
    def cursor(self, results):
        cursor = MagicMock()
        cursor.fetchone.side_effect = results
        cursor.fetchall.return_value = results
        connection = MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.__enter__.return_value = cursor
        with patch.object(incremental_state, 'connect', return_value=connection):
            yield cursor

    def test_dataset_rollover_rejects_daily_before_publication(self):
        with self.cursor([(uuid.UUID(int=2), 10), (uuid.UUID(int=1), 9)]):
            with self.assertRaisesRegex(RuntimeError, 'dataset changed'):
                incremental_state._state_input('silver', 'daily_refresh')

    def test_gold_consumes_only_completed_bounded_publications_for_active_dataset(self):
        dataset = uuid.UUID(int=1)
        with self.cursor([(dataset,10),(dataset,9),None,(dataset,4),(7,)]) as cursor:
            cursor.fetchall.return_value = [(date(2026,2,1),)]
            values = incremental_state._state_input('gold','daily_refresh')
            self.assertEqual(values['GOLD_SILVER_PUBLICATION_TO'], '7')
            self.assertEqual(values['GOLD_SILVER_BATCH_SEQUENCE'], '9')
            self.assertEqual(values['GOLD_AFFECTED_MONTHS'], '2026-02-01')
            query = next(c for c in cursor.execute.call_args_list if 'unnest(p.affected_months)' in c.args[0])
            self.assertIn("p.status='completed'", query.args[0])
            self.assertEqual(query.args[1], (4,7,dataset))

    def test_gold_rejects_partial_silver_even_when_an_older_checkpoint_exists(self):
        dataset = uuid.UUID(int=1)
        with self.cursor([(dataset,10),(dataset,9),(1,)]):
            with self.assertRaisesRegex(RuntimeError, 'incomplete publication'):
                incremental_state._state_input('gold','daily_refresh')

    def test_retry_keeps_old_month_invalidation_after_partial_commit(self):
        publication_id = uuid.UUID(int=9)
        env = {'ICEBERG_DATASET_ID': str(uuid.UUID(int=1)), 'ICEBERG_PUBLICATION_ID': str(uuid.UUID(int=8)),
               'SILVER_DELTA_FROM_SEQUENCE': '1', 'SILVER_DELTA_TO_SEQUENCE': '5'}
        with patch.dict(os.environ, env), self.cursor([(publication_id, [date(2026,1,1)])]) as cursor:
            months = incremental_state.stage_silver_publication({date(2026,2,1)})
            self.assertEqual(months, {date(2026,1,1), date(2026,2,1)})
            self.assertEqual(os.environ['ICEBERG_PUBLICATION_ID'], str(publication_id))
            update = cursor.execute.call_args
            self.assertEqual(update.args[1], ([date(2026,1,1),date(2026,2,1)],1,5,publication_id))


if __name__ == '__main__':
    unittest.main()
