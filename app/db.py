import threading

class Database:
    def __init__(self):
        self.data = {}
        self.lock = threading.Lock()

    def get(self, key):
        with self.lock:
            return self.data.get(key)

    def create(self, key, value):
        with self.lock:
            if key in self.data:
                return False, "Key already exists"
            self.data[key] = value
            return True, None

    def update(self, key, value):
        with self.lock:
            if key not in self.data:
                return False, "Key does not exist"
            self.data[key] = value
            return True, None

    def delete(self, key):
        with self.lock:
            if key in self.data:
                del self.data[key]
                return True, None
            return False, "Key does not exist"

    def compare_and_swap(self, key, old_value, new_value):
        with self.lock:
            if self.data.get(key) == old_value:
                self.data[key] = new_value
                return True, None
            return False, "Value mismatch"
