from http.server import BaseHTTPRequestHandler, HTTPServer


class ReceiverHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers["Content-Length"])
        body = self.rfile.read(content_length)

        print("Receiver listening on port 8000", flush=True)
        print(body.decode())

        self.send_response(200)
        self.end_headers()

        self.wfile.write(b"OK")


server = HTTPServer(("0.0.0.0", 8000), ReceiverHandler)

print("Receiver listening on port 8000", flush=True)

server.serve_forever()