RING_BITS = 32            # use 32-bit ring for easier debugging
RING_SIZE = 2 ** RING_BITS

K_REPLICATION = 3         # number of replicas for each key
SUCCESSOR_LIST_SIZE = 4   # r
STABILIZE_INTERVAL = 3.0  # seconds
FIX_FINGERS_INTERVAL = 5.0
HEARTBEAT_INTERVAL = 3.0
ANTI_ENTROPY_INTERVAL = 10.0

# election timeouts in seconds
ELECTION_TIMEOUT = 5.0

# Gnutella-related
GNUTELLA_TTL_DEFAULT = 5
