import argparse
import subprocess
import time
import random
from typing import List
import requests


def start_node(port: int, bootstrap: str | None) -> subprocess.Popen:
    """
    Start a single Chord+KV+Gnutella node as a subprocess.
    """
    cmd = ["python", "run_node.py", "--host", "127.0.0.1", "--port", str(port)]
    if bootstrap:
        cmd += ["--bootstrap", bootstrap]
    return subprocess.Popen(cmd)


def start_cluster(num_nodes: int, base_port: int) -> List[subprocess.Popen]:
    """
    Start a cluster of num_nodes nodes on consecutive ports.
    First node starts without bootstrap, others join via the first node.
    """
    procs: List[subprocess.Popen] = []
    bootstrap_addr = f"127.0.0.1:{base_port}"

    for i in range(num_nodes):
        port = base_port + i
        bootstrap = None if i == 0 else bootstrap_addr
        p = start_node(port, bootstrap)
        procs.append(p)
        time.sleep(0.5)  # small delay to let each node bind and join

    # Give the ring some time to stabilize
    time.sleep(5.0)
    return procs


def stop_cluster(procs: List[subprocess.Popen]):
    for p in procs:
        try:
            p.terminate()
        except Exception:
            pass
    for p in procs:
        try:
            p.wait(timeout=5.0)
        except Exception:
            pass


def run_workload(num_ops: int, base_port: int, num_nodes: int):
    """
    Simple write+read workload distributed across nodes.

    This is intentionally basic: enough to generate metrics for your report
    (latency, hop counts, etc.) without being tied to a specific test harness.
    """
    ports = [base_port + i for i in range(num_nodes)]
    for i in range(num_ops):
        key = f"key-{i}"
        value = f"value-{i}"
        writer_id = "client0"

        # choose a random node to send the PUT to
        put_port = random.choice(ports)
        put_addr = f"http://127.0.0.1:{put_port}/put"
        try:
            requests.post(
                put_addr,
                json={"key": key, "value": value, "writer_id": writer_id},
                timeout=1.0,
            )
        except Exception:
            pass

        # choose a random node for GET
        get_port = random.choice(ports)
        get_addr = f"http://127.0.0.1:{get_port}/get"
        try:
            requests.post(get_addr, json={"key": key}, timeout=1.0)
        except Exception:
            pass


def collect_metrics(base_port: int):
    """
    Pull metrics from one node and print them. You can extend this to pull
    from all nodes and aggregate.
    """
    url = f"http://127.0.0.1:{base_port}/metrics"
    try:
        r = requests.get(url, timeout=2.0)
        if r.status_code == 200:
            print("=== Metrics from node", base_port, "===")
            print(r.text)
        else:
            print("Failed to fetch metrics:", r.status_code)
    except Exception as e:
        print("Error fetching metrics:", e)


def run_scale_experiment(num_nodes: int, base_port: int, num_ops: int):
    procs = start_cluster(num_nodes, base_port)
    try:
        run_workload(num_ops, base_port, num_nodes)
        collect_metrics(base_port)
    finally:
        stop_cluster(procs)


def run_churn_experiment(num_nodes: int, base_port: int, num_ops: int, churn_interval: float):
    """
    Simple churn experiment:
    - Start a cluster.
    - Periodically kill and restart a random node while running a workload.
    """
    procs = start_cluster(num_nodes, base_port)
    ports = [base_port + i for i in range(num_nodes)]

    start_time = time.time()
    next_churn = start_time + churn_interval

    try:
        i = 0
        while i < num_ops:
            now = time.time()
            if now >= next_churn:
                # kill a random node
                victim_idx = random.randrange(len(procs))
                victim_proc = procs[victim_idx]
                victim_port = ports[victim_idx]
                print(f"[CHURN] Killing node on port {victim_port}")
                try:
                    victim_proc.terminate()
                    victim_proc.wait(timeout=5.0)
                except Exception:
                    pass

                # restart it
                print(f"[CHURN] Restarting node on port {victim_port}")
                new_proc = start_node(victim_port, f"127.0.0.1:{base_port}")
                procs[victim_idx] = new_proc

                next_churn = now + churn_interval

            # one workload op
            key = f"key-{i}"
            value = f"value-{i}"
            writer_id = "client0"

            put_port = random.choice(ports)
            put_addr = f"http://127.0.0.1:{put_port}/put"
            get_port = random.choice(ports)
            get_addr = f"http://127.0.0.1:{get_port}/get"

            try:
                requests.post(
                    put_addr,
                    json={"key": key, "value": value, "writer_id": writer_id},
                    timeout=1.0,
                )
            except Exception:
                pass

            try:
                requests.post(get_addr, json={"key": key}, timeout=1.0)
            except Exception:
                pass

            i += 1

        collect_metrics(base_port)
    finally:
        stop_cluster(procs)


def main():
    parser = argparse.ArgumentParser(description="Chord/KV/Gnutella experiment harness")
    parser.add_argument("--mode", choices=["scale", "churn"], required=True,
                        help="Which experiment to run.")
    parser.add_argument("--num-nodes", type=int, default=5)
    parser.add_argument("--base-port", type=int, default=5000)
    parser.add_argument("--num-ops", type=int, default=100)
    parser.add_argument("--churn-interval", type=float, default=5.0,
                        help="Seconds between churn events (churn mode only).")
    args = parser.parse_args()

    if args.mode == "scale":
        run_scale_experiment(args.num_nodes, args.base_port, args.num_ops)
    else:
        run_churn_experiment(args.num_nodes, args.base_port, args.num_ops, args.churn_interval)


if __name__ == "__main__":
    main()
