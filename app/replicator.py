class Replicator:
    def __init__(self, raft_server):
        self.raft_server = raft_server

    def apply_and_replicate(self, command, key, value=None, old_value=None):
        with self.raft_server.lock:
            if self.raft_server.state != "LEADER":
                raise Exception(f"Node {self.raft_server.node_id} is not the leader.")

            log_entry = {
                "term": self.raft_server.term,
                "command": command,
                "key": key,
                "value": value,
                "old_value": old_value
            }
            self.raft_server.log.append(log_entry)

        ack_count = 1
        for peer in self.raft_server.peers:
            ack = self.send_append_entries(peer, log_entry)
            if ack:
                ack_count += 1

        if ack_count > len(self.raft_server.peers) // 2:
            with self.raft_server.lock:
                self.raft_server.db.process_write(command, key, value, old_value)
                self.raft_server.commit_index += 1
            return True
        return False

    def send_append_entries(self, peer, entry):
        # Simulate sending append entries RPC to peer
        # This should implement actual network communication logic
        return True
