import threading

class LamportClock:
    def __init__(self, initial=0):
        self.time = initial
        self._lock = threading.Lock()

    def tick(self) -> int:
        """Local event."""
        with self._lock:
            self.time += 1
            return self.time

    def update(self, received_ts: int) -> int:
        """On receiving a message with timestamp."""
        with self._lock:
            self.time = max(self.time, received_ts) + 1
            return self.time

    def read(self) -> int:
        with self._lock:
            return self.time
