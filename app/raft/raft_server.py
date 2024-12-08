import threading
from app.db import Database

class LogEntry:
    def __init__(self, term, command, key, value=None, old_value=None):
        """
        Represents a single log entry in the Raft protocol.

        :param term: (int) The term in which this log entry was created.
        :param command: (str) The operation to be executed (e.g., CREATE, UPDATE, DELETE, CAS).
        :param key: (str) The resource identifier this log entry targets.
        :param value: (str, optional) The new value to be set, if applicable.
        :param old_value: (str, optional) The current value for operations like CAS.
        """
        self.term = term
        self.command = command
        self.key = key
        self.value = value
        self.old_value = old_value

    def __repr__(self):
        """
        String representation of the log entry.
        """
        return (f"LogEntry(term={self.term}, command={self.command}, key={self.key}, "
                f"value={self.value}, old_value={self.old_value})")


class RaftServer:
    def __init__(self, node_id, peers, raft_port):
        """
        Initializes a Raft server instance.

        :param node_id: (int) Unique identifier for the node.
        :param peers: (list) List of peer node identifiers.
        :param raft_port: (int) Port used for Raft communication.
        """
        self.node_id = node_id
        self.peers = peers
        self.raft_port = raft_port
        self.state = "FOLLOWER"
        self.term = 0
        self.voted_for = None
        self.commit_index = 0
        self.log = []  # List of LogEntry objects
        self.lock = threading.Lock()
        self.db = Database()
        self.leader_id = None

    def append_log_entry(self, command, key, value=None, old_value=None):
        """
        Appends a new log entry to the log and returns it.

        :param command: (str) The operation to be executed.
        :param key: (str) The resource identifier this log entry targets.
        :param value: (str, optional) The new value to be set, if applicable.
        :param old_value: (str, optional) The current value for operations like CAS.
        :return: (LogEntry) The created log entry.
        """
        with self.lock:
            entry = LogEntry(term=self.term, command=command, key=key, value=value, old_value=old_value)
            self.log.append(entry)
            return entry

    def commit_entry(self, index):
        """
        Commits a log entry by applying it to the state machine.

        :param index: (int) Index of the log entry to commit.
        """
        with self.lock:
            if index < len(self.log):
                entry = self.log[index]
                if entry.command == "CREATE":
                    self.db.create(entry.key, entry.value)
                elif entry.command == "UPDATE":
                    self.db.update(entry.key, entry.value)
                elif entry.command == "DELETE":
                    self.db.delete(entry.key)
                elif entry.command == "CAS":
                    self.db.compare_and_swap(entry.key, entry.old_value, entry.value)
                self.commit_index = index

    def handle_client_request(self, command, key, value=None, old_value=None):
        """
        Handles a client request by creating and appending a log entry.

        :param command: (str) The operation to be executed.
        :param key: (str) The resource identifier this log entry targets.
        :param value: (str, optional) The new value to be set, if applicable.
        :param old_value: (str, optional) The current value for operations like CAS.
        :return: (LogEntry) The created log entry.
        """
        entry = self.append_log_entry(command, key, value, old_value)
        # Log replication to peers would go here (AppendEntries RPC).
        return entry