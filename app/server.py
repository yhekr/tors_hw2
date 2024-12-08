from flask import Flask, request, jsonify

app = Flask(__name__)

def start_http_server(raft_server, port):
    @app.route('/create', methods=['POST'])
    def create():
        data = request.json
        key, value = data["key"], data["value"]
        success, error = raft_server.db.create(key, value)
        if success:
            return jsonify({"message": "Key created"}), 201
        return jsonify({"error": error}), 400

    @app.route('/read/<key>', methods=['GET'])
    def read(key):
        value = raft_server.db.get(key)
        if value is not None:
            return jsonify({"key": key, "value": value})
        return jsonify({"error": "Key not found"}), 404

    @app.route('/update', methods=['POST'])
    def update():
        data = request.json
        key, value = data["key"], data["value"]
        success, error = raft_server.db.update(key, value)
        if success:
            return jsonify({"message": "Key updated"}), 200
        return jsonify({"error": error}), 400

    @app.route('/delete', methods=['POST'])
    def delete():
        data = request.json
        key = data["key"]
        success, error = raft_server.db.delete(key)
        if success:
            return jsonify({"message": "Key deleted"}), 200
        return jsonify({"error": error}), 400

    app.run(port=port)
