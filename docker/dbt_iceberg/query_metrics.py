"""Capture actual DuckDB scans; a SQL filter alone is not file-pruning evidence."""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path


def execute_measured(con, statement: str, label: str):
    if os.getenv('ICEBERG_SCAN_PROFILING', 'false').lower() != 'true':
        return con.execute(statement).fetchall()
    root = Path(os.environ['DUCKDB_PATH']).parent / 'query_profiles'
    root.mkdir(parents=True, exist_ok=True)
    path = root / f'{uuid.uuid4()}.json'
    con.execute("SET profiling_coverage = 'ALL'")
    con.execute("SET enable_profiling = 'json'")
    con.execute(f"SET profiling_output = '{str(path).replace(chr(39), chr(39) * 2)}'")
    try:
        rows = con.execute(statement).fetchall()
    finally:
        con.execute('PRAGMA disable_profiling')
    if not path.exists():
        print(json.dumps({'scan_label': label, 'scan_metrics': 'unavailable'}), flush=True)
        return rows
    profile = json.loads(path.read_text())
    scans = []

    def visit(node):
        if 'SCAN' in (node.get('operator_name', '') + node.get('operator_type', '')).upper():
            extra = node.get('extra_info', {})
            scans.append({
                'operator': node.get('operator_name'),
                'table': extra.get('Table'),
                'rows_scanned': node.get('operator_rows_scanned'),
                'rows_returned': node.get('operator_cardinality'),
                'file_metrics': {k: v for k, v in extra.items() if 'file' in k.lower()},
            })
        for child in node.get('children', []):
            visit(child)

    visit(profile)
    print(json.dumps({
        'scan_label': label, 'elapsed_seconds': profile.get('latency'),
        'peak_buffer_bytes': profile.get('system_peak_buffer_memory'),
        'peak_temp_bytes': profile.get('system_peak_temp_dir_size'),
        'rows_scanned': profile.get('cumulative_rows_scanned'),
        'scans': scans,
        'file_scan_metrics_available': any(
            ('read' in key.lower() or 'scan' in key.lower() or 'count' in key.lower())
            and 'filter' not in key.lower()
            for scan in scans for key in scan['file_metrics']
        ),
        'business_rows_affected': rows[0][0] if statement.lstrip().upper().startswith(('MERGE', 'DELETE')) and len(rows) == 1 and len(rows[0]) == 1 else None,
    }), flush=True)
    return rows
