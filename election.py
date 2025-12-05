import threading
from typing import Dict, Optional
from config import ELECTION_TIMEOUT


class ReplicaElectionState:
    """
    Simple Bully-style election state for a given key's replica set.

    Each node has a numeric priority (e.g., its Chord ID). The Bully
    algorithm's outcome is "the highest-priority live node becomes leader".
    We only keep local state here; the actual "who is alive" logic is done
    in ChordNode using /ping RPCs.
    """
    def __init__(self, node_priority: int):
        # This node's priority (e.g., ring ID)
        self.node_priority = node_priority
        # Priority of the current leader, or None if no leader known
        self.current_leader: Optional[int] = None
        # Whether this node believes an election is in progress for this key
        self.in_election: bool = False
        self.lock = threading.Lock()


class ElectionManager:
    """
    Manage per-key Bully election state.

    ChordNode is responsible for:
      - deciding which replica set applies to a given key,
      - checking which replicas are alive (via /ping),
      - calling start_election_local() when membership changes or a leader dies,
      - calling set_leader() once a winner is determined,
      - consulting get_leader() to reuse an existing leader when still valid.
    """

    def __init__(self, node_priority: int):
        self.node_priority = node_priority
        self.per_key: Dict[str, ReplicaElectionState] = {}
        self.global_lock = threading.Lock()

    def _get_state(self, key: str) -> ReplicaElectionState:
        with self.global_lock:
            if key not in self.per_key:
                self.per_key[key] = ReplicaElectionState(self.node_priority)
            return self.per_key[key]

    def start_election_local(self, key: str):
        """
        Mark that this node has started an election for key.

        The original Bully algorithm would send ELECTION messages to all
        higher-priority replicas and wait up to ELECTION_TIMEOUT for OK
        replies or a COORDINATOR announcement.

        In this project we implement the same *effect* by:
          - asking ChordNode to probe replicas with higher priority,
          - picking the highest-priority live node as leader,
          - calling set_leader() once decided.

        This method only flips the local state to "in_election = True" so
        that ChordNode knows a new election is required.
        """
        state = self._get_state(key)
        with state.lock:
            state.in_election = True

    def set_leader(self, key: str, leader_priority: Optional[int]):
        """
        Record the leader for this key. If leader_priority is None, we clear
        the current leader (e.g., no live replicas found).
        """
        state = self._get_state(key)
        with state.lock:
            state.current_leader = leader_priority
            state.in_election = False

    def get_leader(self, key: str) -> Optional[int]:
        """
        Return the cached leader priority for this key, or None if no leader
        is currently known.
        """
        state = self._get_state(key)
        with state.lock:
            return state.current_leader
