# metrics.py

import threading
import time
from typing import Dict, Any

class Metrics:
    def __init__(self, node_addr: str):
        self.node_addr = node_addr
        self._lock = threading.Lock()

        # KV metrics
        self.total_puts = 0
        self.total_gets = 0
        self.total_get_hits = 0
        self.total_get_misses = 0
        self.sum_put_latency = 0.0
        self.sum_get_latency = 0.0

        # Chord routing metrics
        self.total_chord_lookups = 0
        self.sum_chord_hops = 0

        # Gnutella metrics
        self.total_gnutella_queries = 0
        self.sum_gnutella_forwarded = 0

        # last reset time
        self.start_time = time.time()

    # ---- Helpers for KV ----
    def record_put(self, latency_sec: float):
        with self._lock:
            self.total_puts += 1
            self.sum_put_latency += latency_sec

    def record_get(self, latency_sec: float, hit: bool):
        with self._lock:
            self.total_gets += 1
            self.sum_get_latency += latency_sec
            if hit:
                self.total_get_hits += 1
            else:
                self.total_get_misses += 1

    # ---- Routing ----
    def record_chord_lookup(self, hops: int):
        with self._lock:
            self.total_chord_lookups += 1
            self.sum_chord_hops += hops

    # ---- Gnutella ----
    def record_gnutella_query(self, forwarded: int):
        with self._lock:
            self.total_gnutella_queries += 1
            self.sum_gnutella_forwarded += forwarded

    # ---- Export ----
    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            uptime = time.time() - self.start_time
            avg_put_lat = (self.sum_put_latency / self.total_puts) if self.total_puts else 0.0
            avg_get_lat = (self.sum_get_latency / self.total_gets) if self.total_gets else 0.0
            avg_chord_hops = (self.sum_chord_hops / self.total_chord_lookups) if self.total_chord_lookups else 0.0
            avg_fwd = (self.sum_gnutella_forwarded / self.total_gnutella_queries) if self.total_gnutella_queries else 0.0

            return {
                "node": self.node_addr,
                "uptime_sec": uptime,
                "kv": {
                    "total_puts": self.total_puts,
                    "total_gets": self.total_gets,
                    "total_get_hits": self.total_get_hits,
                    "total_get_misses": self.total_get_misses,
                    "avg_put_latency_sec": avg_put_lat,
                    "avg_get_latency_sec": avg_get_lat,
                },
                "chord": {
                    "total_lookups": self.total_chord_lookups,
                    "avg_hops": avg_chord_hops,
                },
                "gnutella": {
                    "total_queries": self.total_gnutella_queries,
                    "avg_forwarded_per_query": avg_fwd,
                },
            }
