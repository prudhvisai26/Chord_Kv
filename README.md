# Distributed Key–Value Store on Chord  
### Replication • LWW Consistency • Bully Elections • Read-Repair • Anti-Entropy • Gnutella Flooding

## Overview
This project implements a fully decentralized, fault-tolerant distributed key–value store.  
It combines:

- Chord DHT for O(log N) routing  
- Multi-replication (default k=3)  
- Lamport timestamps + Last-Writer-Wins (LWW) conflict resolution  
- Per-key Bully leader elections  
- Read-repair for rapid convergence  
- Anti-entropy background syncing  
- Gnutella flooding as an unstructured search baseline  
- Metrics for hop count, latency, and flooding statistics

The system remains available under node churn, crashes, and restarts without any central coordinator.

## Core Features
### Chord Distributed Hash Table
- 32-bit SHA-1 identifier ring  
- Successor, predecessor, and successor lists for failover  
- Finger table for efficient routing  
- Background stabilization, fix-fingers, and heartbeat threads  

### Replicated Key–Value Store
- k replicas per key  
- Values stored as (value, timestamp, writer_id)  
- LWW ensures deterministic conflict resolution  

### Lamport Clocks
- Logical time ordering for PUTs  
- max(local, remote)+1 rules for merges  

### Per-Key Bully Elections
- Localized elections within replica sets  
- Highest-ID live replica becomes leader  

### Read-Repair
- GET requests repair stale replicas instantly  

### Anti-Entropy
- Periodic full-map synchronization with successors  

### Gnutella Flooding
- TTL-bounded broadcast queries  
- UUID duplicate suppression  

## Architecture
```
Client → ChordNode → Routing → Replica Set → KV Store
                           ↳ Elections
                           ↳ LWW + Lamport
                           ↳ Read-Repair
                           ↳ Anti-Entropy
                           ↳ Gnutella
```

## Running the System
### Terminal 1 — Bootstrap
python run_node.py --host 127.0.0.1 --port 5000

### Terminal 2 — Secondary Nodes
python run_node.py --host 127.0.0.1 --port 5001 --bootstrap 127.0.0.1:5000  
python run_node.py --host 127.0.0.1 --port 5002 --bootstrap 127.0.0.1:5000  
python run_node.py --host 127.0.0.1 --port 5003 --bootstrap 127.0.0.1:5000  

### Terminal 3 — Demo Script
bash run_bash.sh

## API
### PUT
POST /put  
{ "key": "...", "value": "...", "writer_id": "client" }

### GET
POST /get  
{ "key": "..." }

### Metrics
GET /metrics

## Experimentation
python experiment_harness.py --mode scale --num-nodes 5 --num-ops 100

## Future Work
- Persistent storage  
- Merkle-tree anti-entropy  
- Dynamic Gnutella neighbors  
- Vector clocks  

## Contributors
Parhuam Jalalian  
Prudhvi Sai Raj Dasari  
Abdul Sabur Samir Saigani
