# Generate simple REST API server for testing purposes
# Usage: python3 http_server.py
# It should receive POST requests with JSON data and return JSON response


from http.server import BaseHTTPRequestHandler, HTTPServer


class MyHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        print(f'Received POST request: {post_data}')
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"status": "ok"}')


def run(server_class=HTTPServer, handler_class=MyHandler, port=8001):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f'Starting httpd on port {port}')
    httpd.serve_forever()


if __name__ == '__main__':
    run()
