"""Run one phase and report sampled pod memory, spill peak and child RSS."""

from __future__ import annotations

import json
import os
import resource
import subprocess
import sys
import time
from pathlib import Path


def run() -> int:
    phase, *command = sys.argv[1:]
    started = time.monotonic()
    sampled_memory = None
    spill_peak = 0
    cgroup = Path('/sys/fs/cgroup')
    memory_paths = [cgroup / 'memory.current', cgroup / 'memory/memory.usage_in_bytes']
    temp = Path(os.environ['DUCKDB_TEMP_DIRECTORY'])
    def memory_events():
        try:
            return {key: int(value) for key, value in
                    (line.split() for line in (cgroup / 'memory.events').read_text().splitlines())}
        except (OSError, ValueError):
            return {}

    events_before = memory_events()
    process = subprocess.Popen(command)
    while True:
        for path in memory_paths:
            try:
                current = int(path.read_text())
                sampled_memory = max(sampled_memory or 0, current)
                break
            except (OSError, ValueError):
                continue
        size = 0
        for directory, _, files in os.walk(temp):
            for name in files:
                try:
                    size += (Path(directory) / name).stat().st_size
                except OSError:
                    pass
        spill_peak = max(spill_peak, size)
        status = process.poll()
        if status is not None:
            break
        time.sleep(0.5)
    print(json.dumps({
        'phase': phase, 'elapsed_seconds': round(time.monotonic() - started, 3),
        'exit_code': status, 'termination_signal': -status if status < 0 else None,
        'cgroup_sampled_peak_bytes': sampled_memory,
        'cgroup_kernel_peak_bytes': next((int(path.read_text()) for path in (
            cgroup / 'memory.peak', cgroup / 'memory/memory.max_usage_in_bytes'
        ) if path.is_file() and os.access(path, os.R_OK)), None),
        'child_max_rss_bytes': resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss * 1024,
        'duckdb_spill_sampled_peak_bytes': spill_peak,
        'cgroup_event_delta': {key: value - events_before.get(key, value)
                               for key, value in memory_events().items()},
    }), flush=True)
    return status if status >= 0 else 128 - status


if __name__ == '__main__':
    raise SystemExit(run())
