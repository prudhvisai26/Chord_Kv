import threading
import time
import json
import argparse
from typing import List, Optional, Dict, Any
from gnutella import GnutellaMixin
from metrics import Metrics
from config import GNUTELLA_TTL_DEFAULT
import uuid

from flask import Flask, request, jsonify
import requests

from config import (
    RING_BITS, RING_SIZE, SUCCESSOR_LIST_SIZE,
    STABILIZE_INTERVAL, FIX_FINGERS_INTERVAL,
    HEARTBEAT_INTERVAL, ANTI_ENTROPY_INTERVAL,
    K_REPLICATION
)

from util import sha1_int, in_interval
from lamport import LamportClock
from kv_store import KVStore
from election import ElectionManager

app = Flask(__name__)

class ChordNode(GnutellaMixin):
    def __init__(self, host: str, port: int, bootstrap: Optional[str] = None):
        GnutellaMixin.__init__(self)
        self.host = host
        self.port = port
        self.addr = f"{host}:{port}"
        self.id = sha1_int(self.addr)
        self.metrics = Metrics(node_addr=self.addr)

        # chord state
        self.predecessor: Optional[Dict[str, Any]] = None
        self.successor: Dict[str, Any] = {"id": self.id, "addr": self.addr}
        self.successor_list: List[Dict[str, Any]] = [self.successor]

        self.finger: List[Optional[Dict[str, Any]]] = [None] * RING_BITS

        self.kv = KVStore(node_id=self.addr)
        self.clock = LamportClock()
        self.election_mgr = ElectionManager(node_priority=self.id)

        self._stop = threading.Event()

        if bootstrap and bootstrap != self.addr:
            self.join(bootstrap)
        else:
            # first node in ring, successor is self
            self.predecessor = None
            self.successor = {"id": self.id, "addr": self.addr}
            self.successor_list = [self.successor]

        self.g_neighbors = set()
        for s in self.successor_list:
            self.add_neighbor(s["addr"])
        if self.predecessor:
            self.add_neighbor(self.predecessor["addr"])

        # background loops
        threading.Thread(target=self.stabilize_loop, daemon=True).start()
        threading.Thread(target=self.fix_fingers_loop, daemon=True).start()
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
        threading.Thread(target=self.anti_entropy_loop, daemon=True).start()

    # ---------- Networking helpers ----------

    def rpc(self, addr: str, path: str, payload: Dict[str, Any], timeout=2.0) -> Optional[Dict[str, Any]]:
        url = f"http://{addr}{path}"
        try:
            r = requests.post(url, json=payload, timeout=timeout)
            if r.status_code == 200:
                return r.json()
        except Exception:
            return None
        return None
    
    def _is_node_alive(self, addr: str) -> bool:
        if addr == self.addr:
            # We know we're alive if this process is running.
            return True
        res = self.rpc(addr, "/ping", {}, timeout=1.0)
        return res is not None

    def get_replicas_for_key(self, key: str) -> List[Dict[str, Any]]:
        """
        Compute the replica set for a key: owner + up to K_REPLICATION-1 successors.
        This reuses the same logic you used in handle_put previously.
        """
        key_id = sha1_int(key)
        owner = self.find_successor(key_id)
        replicas = [owner]

        current = owner
        while len(replicas) < K_REPLICATION:
            res = self.rpc(current["addr"], "/get_successor", {})
            if not res:
                break
            nxt = res["successor"]
            # avoid loops and duplicates
            if nxt["addr"] == replicas[-1]["addr"]:
                break
            if nxt["addr"] not in [n["addr"] for n in replicas]:
                replicas.append(nxt)
            current = nxt

        return replicas

    def ensure_replica_leader(self, key: str, replicas: List[Dict[str, Any]]) -> Optional[int]:
        """
        Run a simple Bully election for this key over its replica set.

        - Leader = highest-ID replica that responds to /ping.
        - We cache the leader in ElectionManager so subsequent requests avoid
          unnecessary elections.
        - If no live replica is found, we clear the leader.
        """
        if not replicas:
            self.election_mgr.set_leader(key, None)
            return None

        valid_ids = {r["id"] for r in replicas}
        current = self.election_mgr.get_leader(key)

        # If we already have a leader and it's still in the replica set,
        # verify it's alive and reuse it.
        if current is not None and current in valid_ids:
            leader = next((r for r in replicas if r["id"] == current), None)
            if leader and self._is_node_alive(leader["addr"]):
                return current

        # Otherwise, start a new election locally.
        self.election_mgr.start_election_local(key)

        # Bully: highest-priority (largest ID) live node wins.
        for r in sorted(replicas, key=lambda n: n["id"], reverse=True):
            if self._is_node_alive(r["addr"]):
                self.election_mgr.set_leader(key, r["id"])
                return r["id"]

        # No live replicas found; clear leader.
        self.election_mgr.set_leader(key, None)
        return None

    # ---------- Chord core ----------

    def join(self, bootstrap_addr: str):
        """Join existing ring via bootstrap node."""
        # find successor of our id via bootstrap
        res = self.rpc(bootstrap_addr, "/find_successor", {"id": self.id})
        if res:
            self.successor = res["node"]
            self.successor_list = [self.successor]
        else:
            # fallback: become standalone
            self.successor = {"id": self.id, "addr": self.addr}
            self.successor_list = [self.successor]

    # def find_successor(self, key_id: int) -> Dict[str, Any]:
    #     """
    #     Iterative Chord lookup.
    #     """
    #     n = {"id": self.id, "addr": self.addr}
    #     while True:
    #         succ = self.successor
    #         if in_interval(key_id, n["id"], succ["id"], inclusive_right=True):
    #             return succ
    #         # ask closest_preceding_finger
    #         cp = self.closest_preceding_finger_local(key_id)
    #         if cp is None or cp["addr"] == n["addr"]:
    #             # cannot progress, ask successor
    #             res = self.rpc(succ["addr"], "/closest_preceding_or_self", {"id": key_id})
    #             if not res:
    #                 return succ
    #             n = res["node"]
    #         else:
    #             # ask that node
    #             res = self.rpc(cp["addr"], "/closest_preceding_or_self", {"id": key_id})
    #             if not res:
    #                 return cp
    #             n = res["node"]

    def find_successor(self, key_id: int) -> Dict[str, Any]:
        n = {"id": self.id, "addr": self.addr}
        hops = 0
        while True:
            hops += 1
            succ = self.successor
            if in_interval(key_id, n["id"], succ["id"], inclusive_right=True):
                self.metrics.record_chord_lookup(hops)
                return succ
            cp = self.closest_preceding_finger_local(key_id)
            if cp is None or cp["addr"] == n["addr"]:
                res = self.rpc(succ["addr"], "/closest_preceding_or_self", {"id": key_id})
                if not res:
                    self.metrics.record_chord_lookup(hops)
                    return succ
                n = res["node"]
            else:
                res = self.rpc(cp["addr"], "/closest_preceding_or_self", {"id": key_id})
                if not res:
                    self.metrics.record_chord_lookup(hops)
                    return cp
                n = res["node"]


    def closest_preceding_finger_local(self, key_id: int) -> Optional[Dict[str, Any]]:
        for i in reversed(range(RING_BITS)):
            f = self.finger[i]
            if f and in_interval(f["id"], self.id, key_id, inclusive_right=False):
                return f
        return None

    # ---------- Stabilization and maintenance ----------

    def stabilize_loop(self):
        while not self._stop.is_set():
            try:
                self.stabilize_once()
            except Exception:
                pass
            time.sleep(STABILIZE_INTERVAL)

    # def stabilize_once(self):
    #     succ = self.successor
    #     if succ["addr"] == self.addr:
    #         return
    #     res = self.rpc(succ["addr"], "/get_predecessor", {})
    #     x = res.get("predecessor") if res else None
    #     if x and in_interval(x["id"], self.id, succ["id"], inclusive_right=False):
    #         self.successor = x
    #     # notify successor
    #     self.rpc(self.successor["addr"], "/notify", {"node": {"id": self.id, "addr": self.addr}})
    #     # refresh successor list
    #     res2 = self.rpc(self.successor["addr"], "/get_successor_list", {})
    #     if res2 and "successor_list" in res2:
    #         sl = res2["successor_list"]
    #         self.successor_list = [self.successor] + sl[:SUCCESSOR_LIST_SIZE-1]
        
    #     self.g_neighbors = set()
    #     for s in self.successor_list:
    #         self.add_neighbor(s["addr"])
    #     if self.predecessor:
    #         self.add_neighbor(self.predecessor["addr"])

    def stabilize_once(self):
        succ = self.successor

        # If our successor is still ourselves but we have a predecessor,
        # use that predecessor as a starting successor. This lets the
        # first node (bootstrap) discover other nodes.
        if succ["addr"] == self.addr and self.predecessor is not None:
            self.successor = self.predecessor
            succ = self.successor

        # If we still only know ourselves and have no predecessor, we can't
        # learn anything yet.
        if succ["addr"] == self.addr:
            return

        # Ask successor for its predecessor; use it if it's between us and succ.
        res = self.rpc(succ["addr"], "/get_predecessor", {})
        x = res.get("predecessor") if res else None
        if x and in_interval(x["id"], self.id, succ["id"], inclusive_right=False):
            self.successor = x

        # Notify successor
        self.rpc(self.successor["addr"], "/notify", {"node": {"id": self.id, "addr": self.addr}})

        # Refresh successor list from successor
        res2 = self.rpc(self.successor["addr"], "/get_successor_list", {})
        if res2 and "successor_list" in res2:
            sl = res2["successor_list"]
            self.successor_list = [self.successor] + sl[:SUCCESSOR_LIST_SIZE-1]

        # Update Gnutella neighbor set from ring view
        self.g_neighbors = set()
        for s in self.successor_list:
            self.add_neighbor(s["addr"])
        if self.predecessor:
            self.add_neighbor(self.predecessor["addr"])


    def fix_fingers_loop(self):
        i = 0
        while not self._stop.is_set():
            try:
                start = (self.id + 2**i) % RING_SIZE
                succ = self.find_successor(start)
                self.finger[i] = succ
                i = (i + 1) % RING_BITS
            except Exception:
                pass
            time.sleep(FIX_FINGERS_INTERVAL)

    def heartbeat_loop(self):
        while not self._stop.is_set():
            try:
                self.check_successor()
                self.check_predecessor()
            except Exception:
                pass
            time.sleep(HEARTBEAT_INTERVAL)

    def check_successor(self):
        if self.successor["addr"] == self.addr:
            return
        res = self.rpc(self.successor["addr"], "/ping", {})
        if not res:
            # failover to next in successor list
            for n in self.successor_list[1:]:
                res2 = self.rpc(n["addr"], "/ping", {})
                if res2:
                    self.successor = n
                    return
            # if all dead, become own successor
            self.successor = {"id": self.id, "addr": self.addr}
            self.successor_list = [self.successor]

    def check_predecessor(self):
        if not self.predecessor:
            return
        res = self.rpc(self.predecessor["addr"], "/ping", {})
        if not res:
            self.predecessor = None

    def anti_entropy_loop(self):
        while not self._stop.is_set():
            try:
                self.run_anti_entropy_once()
            except Exception:
                pass
            time.sleep(ANTI_ENTROPY_INTERVAL)

    def run_anti_entropy_once(self):
        """
        Simple anti-entropy: send our key-version map to successors
        for keys that this node is responsible for.
        """
        all_kv = self.kv.dump()
        # For simplicity, just send everything to all successors (inefficient but simple)
        for succ in self.successor_list:
            if succ["addr"] == self.addr:
                continue
            self.rpc(succ["addr"], "/replica_sync", {"kv": all_kv})

    # ---------- KV operations with replication ----------

    def handle_put(self, key: str, value: Any, client_ts: Optional[int], writer_id: str):
        start = time.time()
        ts = self.clock.tick() if client_ts is None else self.clock.update(client_ts)

        # Always store locally as before (this node may also be a replica).
        self.kv.put(key, value, ts, writer_id)

        # Compute replica set for this key.
        replicas = self.get_replicas_for_key(key)

        # Trigger Bully election for this key's replica set.
        leader_id = self.ensure_replica_leader(key, replicas)

        # Replicate to all replicas (including leader). This preserves your
        # original replication semantics while adding leader metadata.
        payload = {"key": key, "value": value, "ts": ts, "writer_id": writer_id}
        for r in replicas:
            self.rpc(r["addr"], "/replica_put", payload)

        latency = time.time() - start
        self.metrics.record_put(latency)
        return ts

    # def handle_put(self, key: str, value: Any, client_ts: Optional[int], writer_id: str):
    #     start = time.time()
    #     ts = self.clock.tick() if client_ts is None else self.clock.update(client_ts)
       
    #     # store locally
    #     self.kv.put(key, value, ts, writer_id)

    #     # replicate to K-1 successors
    #     owner = self.find_successor(sha1_int(key))
    #     replicas = [owner]
    #     # simple: just walk successor list via RPC
    #     current = owner
    #     while len(replicas) < K_REPLICATION:
    #         res = self.rpc(current["addr"], "/get_successor", {})
    #         if not res:
    #             break
    #         nxt = res["successor"]
    #         if nxt["addr"] == replicas[-1]["addr"]:
    #             break
    #         if nxt["addr"] not in [n["addr"] for n in replicas]:
    #             replicas.append(nxt)
    #         current = nxt

    #     payload = {"key": key, "value": value, "ts": ts, "writer_id": writer_id}
    #     for r in replicas:
    #         self.rpc(r["addr"], "/replica_put", payload)

    #     latency = time.time() - start
    #     self.metrics.record_put(latency)
    #     return ts

    def handle_get(self, key: str):
        start = time.time()

        # Compute replica set and run Bully election.
        replicas = self.get_replicas_for_key(key)
        leader_id = self.ensure_replica_leader(key, replicas)

        # Prefer to read from the elected leader if we have one, otherwise fall
        # back to the hash owner (first replica).
        leader_node = None
        if leader_id is not None:
            for r in replicas:
                if r["id"] == leader_id:
                    leader_node = r
                    break

        # Build a list of replicas we will query for read-repair:
        # put elected leader first to minimize latency in the common case.
        query_order: List[Dict[str, Any]] = []
        if leader_node is not None:
            query_order.append(leader_node)
        for r in replicas:
            if leader_node is None or r["addr"] != leader_node["addr"]:
                query_order.append(r)

        # Fan-out GET to all replicas (in order), collect all versions.
        best_value = None
        best_ts = -1
        best_writer = ""
        found_any = False
        per_replica_vals: Dict[str, Optional[tuple]] = {}

        for r in query_order:
            res = self.rpc(r["addr"], "/replica_get", {"key": key})
            if not res or not res.get("found", False):
                per_replica_vals[r["addr"]] = None
                continue

            v = res["value"]
            ts = res["ts"]
            w = res["writer_id"]
            per_replica_vals[r["addr"]] = (v, ts, w)
            found_any = True

            # LWW comparison inlined to avoid reaching into KVStore internals.
            if best_value is None or ts > best_ts or (ts == best_ts and w > best_writer):
                best_value, best_ts, best_writer = v, ts, w

        if not found_any:
            latency = time.time() - start
            self.metrics.record_get(latency, hit=False)
            return None

        # Per-GET read-repair: push the freshest value to any stale or missing replicas.
        repair_payload = {
            "key": key,
            "value": best_value,
            "ts": best_ts,
            "writer_id": best_writer,
        }
        for r in replicas:
            entry = per_replica_vals.get(r["addr"])
            if entry is None:
                # Replica missing the key entirely; repair it.
                self.rpc(r["addr"], "/replica_put", repair_payload)
            else:
                v_old, ts_old, w_old = entry
                if best_ts > ts_old or (best_ts == ts_old and best_writer > w_old):
                    # Replica has a stale version; repair it.
                    self.rpc(r["addr"], "/replica_put", repair_payload)

        latency = time.time() - start
        self.metrics.record_get(latency, hit=True)
        return best_value, best_ts, best_writer

    # def handle_get(self, key: str):
    #     start = time.time()
    #     key_id = sha1_int(key)
    #     owner = self.find_successor(key_id)

    #     # ask owner for its view + read-repair
    #     res = self.rpc(owner["addr"], "/replica_get", {"key": key})
    #     if not res:
    #         latency = time.time() - start
    #         self.metrics.record_get(latency, hit=False)
    #         return None
        
    #     if not res.get("found", False):
    #         latency = time.time() - start
    #         self.metrics.record_get(latency, hit=False)
    #         return None
    #     value = res.get("value")
    #     ts = res.get("ts")
    #     writer_id = res.get("writer_id")
    #     latency = time.time() - start
    #     self.metrics.record_get(latency, hit=True)
    #     return value, ts, writer_id

# ---------- Flask routes bound to a global node instance ----------

node: Optional[ChordNode] = None

@app.route("/ping", methods=["POST"])
def ping():
    return jsonify({"ok": True})

@app.route("/get_predecessor", methods=["POST"])
def get_predecessor():
    if node.predecessor:
        return jsonify({"predecessor": node.predecessor})
    return jsonify({"predecessor": None})

@app.route("/notify", methods=["POST"])
def notify():
    n = request.json["node"]
    if (node.predecessor is None) or in_interval(n["id"], node.predecessor["id"], node.id, inclusive_right=False):
        node.predecessor = n
    return jsonify({"ok": True})

@app.route("/get_successor_list", methods=["POST"])
def get_successor_list():
    return jsonify({"successor_list": node.successor_list})

@app.route("/get_successor", methods=["POST"])
def get_successor():
    return jsonify({"successor": node.successor})

@app.route("/find_successor", methods=["POST"])
def find_successor_route():
    key_id = int(request.json["id"])
    succ = node.find_successor(key_id)
    return jsonify({"node": succ})

@app.route("/closest_preceding_or_self", methods=["POST"])
def closest_preceding_or_self():
    key_id = int(request.json["id"])
    cp = node.closest_preceding_finger_local(key_id)
    if cp is None:
        cp = {"id": node.id, "addr": node.addr}
    return jsonify({"node": cp})

# KV routes
@app.route("/put", methods=["POST"])
def put_route():
    body = request.json
    key = body["key"]
    value = body["value"]
    client_ts = body.get("ts")
    writer_id = body.get("writer_id", node.addr)
    ts = node.handle_put(key, value, client_ts, writer_id)
    return jsonify({"ok": True, "ts": ts})

@app.route("/get", methods=["POST"])
def get_route():
    key = request.json["key"]
    res = node.handle_get(key)
    if res is None:
        return jsonify({"found": False})
    value, ts, writer_id = res
    return jsonify({"found": True, "value": value, "ts": ts, "writer_id": writer_id})

@app.route("/replica_put", methods=["POST"])
def replica_put():
    body = request.json
    key = body["key"]
    value = body["value"]
    ts = body["ts"]
    writer_id = body["writer_id"]
    node.clock.update(ts)
    node.kv.put(key, value, ts, writer_id)
    return jsonify({"ok": True})

@app.route("/replica_get", methods=["POST"])
def replica_get():
    key = request.json["key"]
    entry = node.kv.get(key)
    if not entry:
        return jsonify({"found": False})
    value, ts, writer_id = entry
    return jsonify({"found": True, "value": value, "ts": ts, "writer_id": writer_id})

@app.route("/replica_get_local", methods=["POST"])
def replica_get_local():
    body = request.get_json(force=True)
    key = body["key"]
    entry = node.kv.get(key)
    if not entry:
        return jsonify({"found": False})
    value, ts, writer_id = entry
    return jsonify({"found": True, "value": value, "ts": ts, "writer_id": writer_id})


@app.route("/replica_sync", methods=["POST"])
def replica_sync():
    kv = request.json["kv"]
    for key, (value, ts, writer_id) in kv.items():
        node.clock.update(ts)
        node.kv.put(key, value, ts, writer_id)
    return jsonify({"ok": True})

def start_node(host: str, port: int, bootstrap: Optional[str]):
    global node
    node = ChordNode(host=host, port=port, bootstrap=bootstrap)
    app.run(host=host, port=port, threaded=True)

# @app.route("/g_query", methods=["POST"])
# def g_query_route():
#     body = request.json
#     msg_id = body["msg_id"]
#     key = body["key"]
#     ttl = body["ttl"]
#     origin = body["origin"]
#     resp = node.g_query_received(msg_id, key, ttl, origin)
#     return jsonify(resp)

@app.route("/g_query", methods=["POST"])
def g_query_route():
    body = request.get_json(force=True) or {}

    # key is required – this is what we are searching for
    key = body["key"]

    # allow msg_id to be generated if not provided
    msg_id = body.get("msg_id") or str(uuid.uuid4())

    # allow ttl to default if not provided
    ttl = int(body.get("ttl", GNUTELLA_TTL_DEFAULT))

    # allow origin to default to this node if not provided
    origin = body.get("origin", node.addr)

    result = node.g_query_received(msg_id, key, ttl, origin)
    return jsonify(result)



@app.route("/g_start_query", methods=["POST"])
def g_start_query_route():
    body = request.get_json(force=True)
    key = body["key"]
    ttl = int(body.get("ttl", GNUTELLA_TTL_DEFAULT))
    result = node.g_handle_query(key, ttl)
    # g_handle_query already records metrics
    return jsonify(result)

# @app.route("/g_start_query", methods=["POST"])
# def g_start_query_route():
#     key = request.json["key"]
#     ttl = request.json.get("ttl", GNUTELLA_TTL_DEFAULT)
#     resp = node.g_handle_query(key, ttl)
#     return jsonify(resp)

@app.route("/metrics", methods=["GET"])
def metrics_route():
    return jsonify(node.metrics.snapshot())    
