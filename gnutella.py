# import uuid
# import threading
# import time
# from typing import Dict, Any, Set
# from flask import request, jsonify
# import requests

# from config import GNUTELLA_TTL_DEFAULT

# class GnutellaMixin:
#     """
#     Mixin for ChordNode to support simple Gnutella-style flooding.
#     """
#     def __init__(self):
#         self.g_neighbors = set()  # neighbor addresses "host:port"
#         self.g_seen: Set[str] = set()
#         self.g_lock = threading.Lock()

#     def add_neighbor(self, addr: str):
#         self.g_neighbors.add(addr)

#     def g_handle_query(self, search_key: str, ttl: int) -> Dict[str, Any]:
#         """
#         Start a flood: returns a dictionary with stats and matches.
#         Here, 'match' means we have that key locally.
#         """
#         msg_id = str(uuid.uuid4())
#         self.g_seen.add(msg_id)
#         matches = []

#         # local match check
#         if self.kv.get(search_key):
#             matches.append({"addr": self.addr, "key": search_key})

#         stats = {"forwarded": 0}
        

#         if ttl <= 0:
#             return {"matches": matches, "stats": stats}

#         for nb in list(self.g_neighbors):
#             try:
#                 r = requests.post(f"http://{nb}/g_query", json={
#                     "msg_id": msg_id,
#                     "key": search_key,
#                     "ttl": ttl - 1,
#                     "origin": self.addr
#                 }, timeout=1.0)
#                 if r.status_code == 200:
#                     resp = r.json()
#                     stats["forwarded"] += 1 + resp["stats"]["forwarded"]
#                     matches.extend(resp["matches"])
#             except Exception:
#                 continue
        
#         result = {"matches": matches, "stats": stats}
#         self.metrics.record_gnutella_query(stats["forwarded"])

#         return result

#     def g_query_received(self, msg_id: str, key: str, ttl: int, origin: str):
#         with self.g_lock:
#             if msg_id in self.g_seen:
#                 return {"matches": [], "stats": {"forwarded": 0}}
#             self.g_seen.add(msg_id)
#         matches = []
#         if self.kv.get(key):
#             matches.append({"addr": self.addr, "key": key})

#         stats = {"forwarded": 0}
#         if ttl <= 0:
#             return {"matches": matches, "stats": stats}

#         for nb in list(self.g_neighbors):
#             if nb == origin:
#                 continue
#             try:
#                 r = requests.post(f"http://{nb}/g_query", json={
#                     "msg_id": msg_id,
#                     "key": key,
#                     "ttl": ttl - 1,
#                     "origin": self.addr
#                 }, timeout=1.0)
#                 if r.status_code == 200:
#                     resp = r.json()
#                     stats["forwarded"] += 1 + resp["stats"]["forwarded"]
#                     matches.extend(resp["matches"])
#             except Exception:
#                 continue

#         return {"matches": matches, "stats": stats}



# gnutella.py

import uuid
import threading
import time
from typing import Dict, Any, Set
import requests

class GnutellaMixin:
    """
    Mixin for ChordNode to support simple Gnutella-style flooding.
    Requires:
      - self.addr (node address "host:port")
      - self.kv.get(key) -> triple or None
      - self.metrics.record_gnutella_query(forwarded)
    """
    def __init__(self):
        self.g_neighbors: Set[str] = set()  # neighbor addresses "host:port"
        self.g_seen: Set[str] = set()
        self.g_lock = threading.Lock()

    def add_neighbor(self, addr: str):
        if addr != self.addr:
            self.g_neighbors.add(addr)

    def _have_key(self, key: str) -> bool:
        # adjust if your KV API differs
        return self.kv.get(key) is not None

    def g_handle_query(self, search_key: str, ttl: int) -> Dict[str, Any]:
        """
        Start a new flood from this node.
        Returns {"matches": [...], "stats": {"forwarded": int}}.
        """
        msg_id = str(uuid.uuid4())
        with self.g_lock:
            self.g_seen.add(msg_id)

        matches = []
        if self._have_key(search_key):
            matches.append({"addr": self.addr, "key": search_key})

        stats = {"forwarded": 0}
        if ttl <= 0:
            self.metrics.record_gnutella_query(stats["forwarded"])
            return {"matches": matches, "stats": stats}

        for nb in list(self.g_neighbors):
            try:
                r = requests.post(
                    f"http://{nb}/g_query",
                    json={"msg_id": msg_id, "key": search_key, "ttl": ttl - 1, "origin": self.addr},
                    timeout=1.0,
                )
                if r.status_code == 200:
                    resp = r.json()
                    stats["forwarded"] += 1 + resp["stats"]["forwarded"]
                    matches.extend(resp["matches"])
            except Exception:
                continue

        self.metrics.record_gnutella_query(stats["forwarded"])
        return {"matches": matches, "stats": stats}

    def g_query_received(self, msg_id: str, key: str, ttl: int, origin: str) -> Dict[str, Any]:
        with self.g_lock:
            if msg_id in self.g_seen:
                return {"matches": [], "stats": {"forwarded": 0}}
            self.g_seen.add(msg_id)

        matches = []
        if self._have_key(key):
            matches.append({"addr": self.addr, "key": key})

        stats = {"forwarded": 0}
        if ttl <= 0:
            return {"matches": matches, "stats": stats}

        for nb in list(self.g_neighbors):
            if nb == origin:
                continue
            try:
                r = requests.post(
                    f"http://{nb}/g_query",
                    json={"msg_id": msg_id, "key": key, "ttl": ttl - 1, "origin": self.addr},
                    timeout=1.0,
                )
                if r.status_code == 200:
                    resp = r.json()
                    stats["forwarded"] += 1 + resp["stats"]["forwarded"]
                    matches.extend(resp["matches"])
            except Exception:
                continue

        return {"matches": matches, "stats": stats}
