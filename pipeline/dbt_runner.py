#!/usr/bin/env python3
"""
pipeline/dbt_runner.py
DAG Derivation and dbt JSON Log Streaming

Provides derive_execution_order() (topological sort from manifest.json)
and stream_dbt_layer() (real-time dbt event streaming via --log-format json).
"""

import json
import os
import subprocess
from collections import defaultdict, deque
from io import TextIOBase


class CompileError(Exception):
    """Raised when dbt compile fails."""
    pass


def derive_execution_order() -> dict:
    """
    Derive model execution order from the dbt DAG.
    Returns {"silver": [names], "gold": [names]} in topological order.
    """
    default_vars = {
        "target_date": "2024-01-01",
        "run_id": "manifest-parse",
        "target_weeks": "[]",
        "s3_bucket": os.environ.get("S3_BUCKET", "cc-transaction-databricks-datalake-2026")
    }
    result = subprocess.run(
        ['dbt', 'compile', '--project-dir', '/app/dbt', '--profiles-dir', '/app/dbt',
         '--vars', json.dumps(default_vars)],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise CompileError(result.stderr)

    with open('/app/dbt/target/manifest.json') as f:
        manifest = json.load(f)

    nodes = {
        key.split('.')[-1]: node
        for key, node in manifest['nodes'].items()
        if key.startswith('model.')
    }

    graph = defaultdict(list)
    in_degree = defaultdict(int)
    for name in nodes:
        in_degree[name] = 0

    for name, node in nodes.items():
        for dep in node['depends_on']['nodes']:
            dep_name = dep.split('.')[-1]
            if dep_name in nodes:
                graph[dep_name].append(name)
                in_degree[name] += 1

    queue = deque([name for name in nodes if in_degree[name] == 0])
    topo_order = []
    while queue:
        current = queue.popleft()
        topo_order.append(current)
        for neighbor in graph[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    silver = [name for name in topo_order if 'silver' in nodes[name].get('tags', [])]
    gold = [name for name in topo_order if 'gold' in nodes[name].get('tags', [])]

    return {"silver": silver, "gold": gold}


def stream_dbt_layer(tag: str, run_id: str, model_vars: dict):
    """
    Run dbt for one layer and yield per-model events as they arrive.
    Yields {"event": "start"/"finish"/"exit", ...} for real-time streaming.
    Field paths are version-specific to dbt-core 1.8.8+ (dbt-duckdb 1.8.0+).
    """
    cmd = [
        'dbt', 'run',
        '--project-dir', '/app/dbt',
        '--profiles-dir', '/app/dbt',
        '--select', tag,
        '--vars', json.dumps(model_vars),
        '--log-format', 'json'
    ]

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    for line in proc.stdout:
        try:
            event = json.loads(line)
            info_name = event.get('info', {}).get('name')
            if info_name == 'LogStartLine':
                node_info = event.get('data', {}).get('node_info', {})
                yield {
                    "event": "start",
                    "model": node_info.get('node_name'),
                    "started_at": node_info.get('node_started_at')
                }
            elif info_name == 'LogModelResult':
                node_info = event.get('data', {}).get('node_info', {})
                yield {
                    "event": "finish",
                    "model": node_info.get('node_name'),
                    "status": node_info.get('node_status'),
                    "started_at": node_info.get('node_started_at'),
                    "completed_at": node_info.get('node_finished_at')
                }
        except (json.JSONDecodeError, KeyError, TypeError):
            pass

    proc.wait()
    yield {"event": "exit", "returncode": proc.returncode}
