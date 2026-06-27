import unittest
import threading
import sys
import os
from unittest.mock import patch

# adjust path to import main from src/
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
from main import JSON




class Tests(unittest.TestCase):

    def test_json(self):
        # grabs a dynamic port from the OS
        server = JSON.server(port=0, clients=2)


        client_1 = JSON.client(host="0.0.0.0", port=server.port)
        client_2 = JSON.client(host="0.0.0.0", port=server.port)

        payload = {
            "name": "Johnson",
            "age": 18
        }


        # start server in the background
        server_thread = threading.Thread(target=lambda: server.send(payload))
        server_thread.start()


        # capture received outputs
        res = {}
        c1_thread = threading.Thread(target=lambda: res.update({"c1": client_1.receive()}))
        c2_thread = threading.Thread(target=lambda: res.update({"c2": client_2.receive()}))

        c1_thread.start()
        c2_thread.start()


        # sync operations
        c1_thread.join()
        c2_thread.join()
        server_thread.join()

        received_1 = res["c1"]
        received_2 = res["c2"]


        self.assertEqual(received_1["name"], "Johnson")
        self.assertEqual(received_2["name"], "Johnson")
        self.assertEqual(received_1, received_2)


if __name__ == "__main__":
    unittest.main()
