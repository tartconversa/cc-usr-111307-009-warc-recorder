import json
import threading
import unittest
from urllib.request import urlopen
from src import source

class SupplierSourceTests(unittest.TestCase):
    def setUp(self): self.source = source.Source()
    def route(self, path, headers=None): return self.source.route('GET', path, headers or {}, b'')
    def test_protocol(self):
        import gzip
        raw = self.route('/page')[2]
        compressed = self.route('/compressed')
        self.assertEqual(gzip.decompress(compressed[2]), raw)
        self.assertEqual(self.route('/jump')[1]['Location'], '/page')
        self.assertEqual(self.route('/chunked')[1]['Transfer-Encoding'], 'chunked')
        self.assertLess(int(self.route('/truncated')[1]['X-Source-Cut']), len(raw))
    def test_http_state_endpoint(self):
        with source.make_server() as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
            try:
                with urlopen('http://127.0.0.1:%d/__state' % server.server_port, timeout=5) as response:
                    self.assertEqual(json.load(response)['generation'], 1)
            finally:
                server.shutdown(); thread.join()

if __name__ == '__main__': unittest.main()
