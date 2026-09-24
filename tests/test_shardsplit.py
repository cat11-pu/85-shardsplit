import json
import threading
import unittest
import urllib.error
import urllib.request

from shardsplit import Shards
from server import serve

class TestShards(unittest.TestCase):
    def test_put_counts(self):
        shards = Shards()
        self.assertEqual(shards.put("a", "1")["shards"], 1)

    def test_get_value(self):
        shards = Shards()
        shards.put("a", "1")
        self.assertEqual(shards.get("a")["value"], "1")

    def test_get_missing(self):
        self.assertIsNone(Shards().get("zz")["value"])

    def test_stats_shape(self):
        self.assertIn("splits", Shards().stats())

    def test_http_put_get(self):
        server = serve(0)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        base = "http://127.0.0.1:%d" % server.server_port
        urllib.request.urlopen(base + "/put", data=b'{"key": "a", "value": "1"}', timeout=5).read()
        with urllib.request.urlopen(base + "/get", data=b'{"key": "a"}', timeout=5) as response:
            self.assertEqual(json.loads(response.read())["value"], "1")
        server.shutdown()
