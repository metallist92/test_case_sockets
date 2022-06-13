import datetime
import socket
import enum
from pymemcache.client import base


class SocketServer:

    def __init__(self):
        self.memcached_client = self.return_memcached_client()
        self.tokens = self.read_tokens()
        self.socket_server = self.return_socket_server_instance()
        self.rate_limit = 60

    def return_memcached_client(self):
        memcached_client = base.Client(('localhost', 11211))
        return memcached_client

    def return_socket_server_instance(self):
        socket_server_ip = '0.0.0.0'
        socket_server_port = 8080
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.settimeout(1)
        server_socket.bind((socket_server_ip, socket_server_port))
        server_socket.listen()
        return server_socket

    def run(self):
        while True:
            try:
                client_socket, addr = self.socket_server.accept()
                client_ip, client_port = addr
                response = self.get_response(client_ip, client_socket.recv(1024))
                client_socket.sendall(response)
                client_socket.close()
            except KeyboardInterrupt:
                self.socket_server.close()
                break
            except socket.timeout:
                continue

    def get_response(self, client_ip, request):
        token = self.return_request_token(request)
        response = None
        if token:
            if token in self.tokens:
                response = Response.OK
            else:
                response = Response.FORBIDDEN
        else:
            if not self.is_threshold_excess(client_ip):
                response = Response.NOT_AUTHORIZED
            else:
                response = Response.TO_MANY_REQUESTS
        return response.encode()

    def is_threshold_excess(self, client_ip):
        counter_key = f'threshold_{client_ip}_{datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}'
        self.memcached_client.add(counter_key, 0, expire=60)
        self.memcached_client.incr(counter_key, 1)
        older_keys = [f'threshold_{client_ip}_{(datetime.datetime.now()-datetime.timedelta(seconds=count)).strftime("%Y-%m-%d_%H-%M-%S")}' for count in range(60)]
        old_memcached_counters = self.memcached_client.get_multi(tuple(older_keys))
        ip_rate = 0
        for old_cached_counter in old_memcached_counters:
            ip_rate += int(old_memcached_counters.get(old_cached_counter))
        if ip_rate > self.rate_limit:
            return True
        return False

    def read_tokens(self):
        tokens = ()
        with open('./tokens.txt', 'r') as token_file:
            tokens = tuple(token_file.read().splitlines())
        return tokens

    def return_request_token(self, request):
        request = request.decode('utf-8')
        request_lines = str(request).splitlines()
        for request_line in request_lines:
            if 'Authorization: Bearer' in request_line:
                return request_line.split()[2]
        return None


class Response(str, enum.Enum):
    NOT_AUTHORIZED = 'HTTP/1.1 401 NOT AUTHORIZED\n\n401 NOT AUTHORIZED'
    FORBIDDEN = 'HTTP/1.1 403 FORBIDDEN\n\n403 FORBIDDEN'
    OK = 'HTTP/1.1 200 OK\n\nIt\'s OK!'
    TO_MANY_REQUESTS = 'HTTP/1.1 429 TO MANY REQUESTS\n\n429 TO MANY REQUESTS'


if __name__ == '__main__':
    socket_server = SocketServer()
    socket_server.run()
