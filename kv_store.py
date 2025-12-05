import threading
from typing import Any, Dict, Tuple, Optional

class KVStore:
    """
    In-memory key-value store with LWW semantics.
    value_map: key -> (value, lamport_ts, writer_id)
    """
    def __init__(self, node_id: str):
        self.node_id = node_id
        self._lock = threading.Lock()
        self.store: Dict[str, Tuple[Any, int, str]] = {}

    def _better(self, old_ts: int, old_writer: str, new_ts: int, new_writer: str) -> bool:
        """Return True if new version should win."""
        if new_ts > old_ts:
            return True
        if new_ts == old_ts and new_writer > old_writer:
            return True
        return False

    def put(self, key: str, value: Any, lamport_ts: int, writer_id: str):
        with self._lock:
            if key not in self.store:
                self.store[key] = (value, lamport_ts, writer_id)
            else:
                old_val, old_ts, old_writer = self.store[key]
                if self._better(old_ts, old_writer, lamport_ts, writer_id):
                    self.store[key] = (value, lamport_ts, writer_id)

    def get(self, key: str) -> Optional[Tuple[Any, int, str]]:
        with self._lock:
            return self.store.get(key)

    def dump(self) -> Dict[str, Tuple[Any, int, str]]:
        with self._lock:
            # shallow copy
            return dict(self.store)
