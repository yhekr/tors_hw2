import threading
import random
import time

class Election:
    def __init__(self, raft_server):
        self.raft_server = raft_server

    def start_election(self):
        with self.raft_server.lock:
            self.raft_server.state = "CANDIDATE"
            self.raft_server.term += 1
            self.raft_server.voted_for = self.raft_server.node_id
            votes_received = 1

        for peer in self.raft_server.peers:
            if self.request_vote(peer):
                votes_received += 1

        if votes_received > len(self.raft_server.peers) // 2:
            self.become_leader()

    def request_vote(self, peer):
        # Simulate sending a vote request to peer
        # This should implement actual network communication logic
        return True

    def become_leader(self):
        with self.raft_server.lock:
            self.raft_server.state = "LEADER"
            self.raft_server.leader_id = self.raft_server.node_id
        self.raft_server.start_heartbeat()

    def reset_election_timer(self):
        timeout = random.uniform(5, 10)
        threading.Timer(timeout, self.start_election).start()
