from abc import ABC, abstractmethod
from contextlib import suppress
import json, os, random, socket, struct, sys, threading, time



class Config:
    def __init__(self, host=None, port=8080, clients=1):
        self.host = host
        self.port = port
        self.clients = clients


    @classmethod
    def from_cli(cls):
        port = 8080
        host = None
        clients = 1

        for a in sys.argv:
            if a.startswith("--port="):
                port = int(a.split("=", 1)[1])

            elif a.startswith("--host="):
                host = a.split("=", 1)[1]

            elif a.startswith("--clients="):
                clients = int(a.split("=", 1)[1])

        return cls(host=host, port=port, clients=clients)



class Envelope(ABC):
    def __init__(self, config: Config):
        self.port = config.port
        self.host = config.host
        self.expected_subscribers = config.clients
        self._clients = []
        self._recv_conn = None
        self._lock = threading.Lock()

        if self.host:
            print(f"Running as CLIENT. Will connect to {self.host}:{self.port}...")

        else:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind(("0.0.0.0", self.port))
            # look up port assigned by the OS (crucial when port is 0 for test files)
            self.port = self.server_socket.getsockname()[1]

            self.server_socket.listen(5)

            print(f"Running as SERVER. Listening on port {self.port} (expecting {self.expected_subscribers} clients)")

            threading.Thread(target=self._accept_loop, daemon=True).start()



    @classmethod
    def from_cli(cls):
        cfg = Config.from_cli()
        return cls(cfg)

    @classmethod
    def server(cls, port=8080, clients=1):
        return cls(Config(host=None, port=port, clients=clients))

    @classmethod
    def client(cls, host, port=8080):
        return cls(Config(host=host, port=port, clients=1))

    @abstractmethod
    def encode(self, value) -> bytes:
        pass

    @abstractmethod
    def decode(self, data):
        pass



    def _recv_n(self, conn, n):
        data = b""
        while len(data) < n:
            chunk = conn.recv(n - len(data))
            if not chunk:
                break
            data += chunk
        return data



    def _close(self, conn):
        with suppress(Exception):
            conn.close()



    def _publish(self, payload):
        while True:
            with self._lock:
                if len(self._clients) >= self.expected_subscribers:
                    break

        with self._lock:
            alive = []
            for conn in self._clients:
                try:
                    conn.sendall(struct.pack("<I", len(payload)))
                    conn.sendall(payload)
                    if conn.recv(1) == b"\x06":
                        alive.append(conn)
                    else:
                        self._close(conn)
                except Exception:
                    self._close(conn)
            self._clients = alive



    def _accept_loop(self):
        while True:
            conn, _ = self.server_socket.accept()
            try:
                mode = conn.recv(1)

                if mode == b"\x01":
                    payload_len = struct.unpack("<I", self._recv_n(conn, 4))[0]
                    payload = self._recv_n(conn, payload_len)
                    self._publish(payload)
                    conn.sendall(b"\x06")
                    self._close(conn)

                elif mode == b"\x02":
                    with self._lock:
                        self._clients.append(conn)

                else:
                    self._close(conn)

            except Exception:
                self._close(conn)



    def send(self, value):
        payload = self.encode(value)

        if payload is None:
            print("Error: Encoding returned None.", file=sys.stderr)
            return value


        if self.host:
            try:
                with socket.create_connection((self.host, self.port)) as conn:
                    conn.sendall(b"\x01")
                    length_header = struct.pack("<I", len(payload))
                    conn.sendall(length_header)
                    conn.sendall(payload)
                    conn.recv(1)
            except Exception as e:
                print(f"Network error: failed during send ({e}).", file=sys.stderr)
        else:
            self._publish(payload)

        return value



    def receive(self):
        if not self.host:
            return None

        try:
            if self._recv_conn is None:
                self._recv_conn = socket.create_connection((self.host, self.port))
                self._recv_conn.sendall(b"\x02")

            conn = self._recv_conn
            payload_len = struct.unpack("<I", self._recv_n(conn, 4))[0]
            payload = self._recv_n(conn, payload_len)
            conn.sendall(b"\x06")
            return self.decode(payload)

        except Exception as e:
            print(f"Network error: failed during receive ({e}).", file=sys.stderr)
            with suppress(Exception):
                if self._recv_conn:
                    self._recv_conn.close()
            self._recv_conn = None
            return None




class Float(Envelope):
    def encode(self, value):
        return struct.pack("<d", value)

    def decode(self, data):
        return struct.unpack("<d", data)[0]




class JSON(Envelope):
    def encode(self, value):
        return json.dumps(value).encode("utf-8")

    def decode(self, data):
        return json.loads(data.decode("utf-8"))




def main():
    envelope = JSON.from_cli()

    while True:
        value = envelope.receive()
        if value:
            print(f"received: {value}")

        """
        age = random.randint(0, 100)
        test_payload = {"name": "test", "age": age}
        print(f"sending: {test_payload}")
        envelope.send(test_payload)
        time.sleep(0.5)
        """


if __name__ == "__main__":
    main()
