import argparse
from chord_node import start_node

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--bootstrap", type=str, default=None,
                        help="bootstrap address host:port (optional)")
    args = parser.parse_args()
    start_node(args.host, args.port, args.bootstrap)

if __name__ == "__main__":
    main()
