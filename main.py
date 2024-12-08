import argparse
from app.raft.raft_server import RaftServer
from app.server import start_http_server

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start Raft node.")
    parser.add_argument("--id", type=int, required=True, help="Node ID")
    parser.add_argument("--port", type=int, required=True, help="HTTP server port")
    parser.add_argument("--raft-port", type=int, required=True, help="Raft communication port")
    parser.add_argument("peers", nargs="*", help="List of peer nodes in the format host:port")

    args = parser.parse_args()

    raft_server = RaftServer(node_id=args.id, peers=args.peers, raft_port=args.raft_port)
    start_http_server(raft_server, args.port)
