"""Compare a full-history local reference with published tables using SELECT only."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import duckdb

from duckdb_runtime import runtime_settings
from publish_catalog import CONTRACT, attach_catalogs, identifier, publish_models, _assert_contract_schema


def compare_table(con, source: str, target: str, columns: list[str]) -> dict:
    selected = ', '.join(identifier(name) for name in columns)
    result = {}
    for label, left, right in (('missing_rows', source, target), ('extra_rows', target, source)):
        result[label] = con.execute(
            f'SELECT count(*) FROM (SELECT {selected} FROM {left} EXCEPT ALL SELECT {selected} FROM {right})'
        ).fetchone()[0]
    date_column = next((name for name in ('month', 'order_date', 'status_datetime', 'budget_date', 'effective_from') if name in columns), None)
    grain = ([f"date_trunc('month', {identifier(date_column)})::date"] if date_column else [])
    grain += [identifier(name) for name in ('store_id', 'product_id') if name in columns]
    group = ', '.join(grain)
    prefix = group + ', ' if group else ''
    group_by = f' GROUP BY {group}' if group else ''

    def aggregate(relation):
        return f'SELECT {prefix}count(*) AS rows, sum(hash({selected})::HUGEINT) AS signature FROM {relation}{group_by}'

    result['different_aggregate_groups'] = con.execute(
        f'SELECT count(*) FROM (({aggregate(source)} EXCEPT {aggregate(target)}) '
        f'UNION ALL ({aggregate(target)} EXCEPT {aggregate(source)}))'
    ).fetchone()[0]
    return result


def validate(layer: str) -> None:
    if os.getenv('ICEBERG_RUN_MODE') != 'full_history':
        raise ValueError('Read-only reference validation requires full_history mode')
    workspace = Path(os.environ['DUCKDB_PATH'])
    manifest = Path(os.environ['DBT_PROJECT_DIR']) / 'target/manifest.json'
    failed = []
    with duckdb.connect(':memory:', config=runtime_settings(workspace)) as con:
        attach_catalogs(con, workspace, (layer,))
        for table in publish_models(manifest, layer):
            source = '.'.join(map(identifier, (f'staged_{layer}', layer, table)))
            target = '.'.join(map(identifier, (f'publish_{layer}', layer, table)))
            _assert_contract_schema(con, source, layer, table)
            columns = [c['name'] for c in CONTRACT.table(layer, table).columns
                       if not c['name'].startswith(('_silver_', '_gold_'))]
            result = compare_table(con, source, target, columns)
            print(json.dumps({'reference_table': f'{layer}.{table}', **result}), flush=True)
            if any(result.values()):
                failed.append(table)
    if failed:
        raise RuntimeError(f'Published tables differ from the read-only full-history reference: {failed}')
    print(f'Read-only {layer} full-history parity passed; no publication/checkpoint performed', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--layer', required=True, choices=('silver', 'gold'))
    validate(parser.parse_args().layer)
