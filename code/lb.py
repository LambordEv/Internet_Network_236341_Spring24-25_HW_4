#!/usr/bin/env python3
import socket
import threading
import time
import sys

# --- Configuration ---
LISTENING_HOST = '10.0.0.1'
LISTENING_PORT = 80
SERVERS = {
    'serv1': {'host': '192.168.0.101', 'port': 80, 'type': 'VIDEO'},
    'serv2': {'host': '192.168.0.102', 'port': 80, 'type': 'VIDEO'},
    'serv3': {'host': '192.168.0.103', 'port': 80, 'type': 'MUSIC'},
}
TIME_MULTIPLIERS = {
    ('VIDEO', 'M'): 2, ('VIDEO', 'V'): 1, ('VIDEO', 'P'): 1,
    ('MUSIC', 'M'): 1, ('MUSIC', 'V'): 3, ('MUSIC', 'P'): 2,
}
BUFFER_SIZE = 2048

# --- Global Shared State & Debug Flag ---
server_sockets = {}
server_finish_times = {}
active_servers = SERVERS.copy()
state_lock = threading.Lock()
DEBUG_MODE = False


def debug_print(*args, **kwargs):
    if DEBUG_MODE:
        print(*args, **kwargs)


def get_estimated_processing_time(server_type, request):
    request_type = request[0]
    base_duration = int(request[1])
    multiplier = TIME_MULTIPLIERS.get((server_type, request_type), 1)
    return base_duration * multiplier


def handle_client(client_conn, client_addr):
    """This function runs in a dedicated thread for each connected client."""
    debug_print("Accepted connection from client {}, starting new thread.".format(client_addr))
    try:
        request = client_conn.recv(BUFFER_SIZE)
        if not request:
            return

        request_str = request.decode().strip()

        best_server_name = None
        chosen_server_ip = None
        chosen_server_socket = None

        with state_lock:
            if not active_servers:
                print("No active servers available. Dropping request.")
                return

            earliest_finish_time = float('inf')
            current_time = time.time()

            for name, config in active_servers.items():
                start_time = max(current_time, server_finish_times.get(name, 0))
                processing_time = get_estimated_processing_time(config['type'], request_str)
                finish_time = start_time + processing_time

                if finish_time < earliest_finish_time:
                    earliest_finish_time = finish_time
                    best_server_name = name
                    chosen_server_ip = config['host']

            if best_server_name:
                server_finish_times[best_server_name] = earliest_finish_time
                chosen_server_socket = server_sockets.get(best_server_name)

        if chosen_server_socket and best_server_name:
            print("received request {} from {} sending to {}-----".format(request_str, client_addr[0], chosen_server_ip))

            try:
                chosen_server_socket.sendall(request)
                response = chosen_server_socket.recv(BUFFER_SIZE)
                client_conn.sendall(response)
                debug_print("Successfully relayed response for request '{}' to client {}".format(request_str, client_addr))

            except socket.error as e:
                print("Error communicating with server {}: {}. Removing from pool.".format(best_server_name, e))
                with state_lock:
                    if best_server_name in active_servers:
                        del active_servers[best_server_name]
                        server_sockets[best_server_name].close()
                        del server_sockets[best_server_name]
        else:
            print("Could not find a suitable server for request '{}'.".format(request_str))

    except socket.error as e:
        print("Socket error with client {}: {}".format(client_addr, e))
    finally:
        client_conn.close()
        debug_print("Closed connection and thread for client {}.".format(client_addr))


def main():
    global DEBUG_MODE
    if "-debug" in sys.argv:
        DEBUG_MODE = True
        debug_print("--- Debug mode enabled ---")

    print("Connecting to servers-----")

    for name, config in SERVERS.items():
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((config['host'], config['port']))
            server_sockets[name] = sock
            server_finish_times[name] = time.time()
            debug_print("Successfully connected to server {}.".format(name))
        except socket.error as e:
            print("Error: Could not connect to server {}. Removing from active pool.".format(name, e))
            if name in active_servers:
                del active_servers[name]

    if not active_servers:
        print("Fatal: Could not connect to any backend servers. Exiting.")
        return

    listening_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listening_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listening_socket.bind((LISTENING_HOST, LISTENING_PORT))
    listening_socket.listen(10)
    debug_print("Load Balancer is listening on {}:{}".format(LISTENING_HOST, LISTENING_PORT))

    try:
        while True:
            client_conn, client_addr = listening_socket.accept()
            thread = threading.Thread(target=handle_client, args=(client_conn, client_addr))
            thread.daemon = True
            thread.start()
    except KeyboardInterrupt:
        debug_print("\n--- Shutting down Load Balancer ---")
    finally:
        listening_socket.close()
        for sock in server_sockets.values():
            sock.close()
        debug_print("All sockets closed.")


if __name__ == "__main__":
    main()
