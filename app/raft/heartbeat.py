import threading

class Heartbeat:
    def __init__(self, raft_server):
        self.raft_server = raft_server
        self.heartbeat_interval = 2  # Seconds

    def send_heartbeat(self):
        with self.raft_server.lock:
            if self.raft_server.state != "LEADER":
                return

        for peer in self.raft_server.peers:
            self.send_append_entries(peer)

        threading.Timer(self.heartbeat_interval, self.send_heartbeat).start()

    def send_append_entries(self, peer):
        # Simulate sending a heartbeat (append entries with no data) to peer
        # This should implement actual network communication logic
        pass
