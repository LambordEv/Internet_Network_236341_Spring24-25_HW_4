#!/usr/bin/env python3
import socket
import select
import time

# --- Configuration ---
# The LB listens on this address. Clients connect here.
LISTENING_HOST = '10.0.0.1'
LISTENING_PORT = 80

# Backend server addresses and types
# These are the servers the LB will connect to.
SERVERS = {
    'serv1': {'host': '192.168.0.101', 'port': 80, 'type': 'VIDEO'},
    'serv2': {'host': '192.168.0.102', 'port': 80, 'type': 'VIDEO'},
    'serv3': {'host': '192.168.0.103', 'port': 80, 'type': 'MUSIC'},
}

# Request processing time multipliers based on server and request type
# This is crucial for our scheduling logic.
# (Server Type, Request Type) -> Multiplier
TIME_MULTIPLIERS = {
    ('VIDEO', 'M'): 2,
    ('VIDEO', 'V'): 1,
    ('VIDEO', 'P'): 1,
    ('MUSIC', 'M'): 1,
    ('MUSIC', 'V'): 3,
    ('MUSIC', 'P'): 2,
}

BUFFER_SIZE = 2048 # Bytes

# --- Main Load Balancer Logic ---

def get_estimated_processing_time(server_type, request):
    """Calculates how long a request will take on a given server type."""
    if len(request) < 2:
        return 0 # Invalid request
    
    request_type = request[0]
    base_duration = int(request[1])
    
    multiplier = TIME_MULTIPLIERS.get((server_type, request_type), 1)
    return base_duration * multiplier

def main():
    """
    Main function to run the Load Balancer.
    - Connects to all backend servers.
    - Listens for client connections.
    - Uses select() to handle all I/O concurrently.
    - Implements a 'Least Estimated Completion Time' scheduling policy.
    """
    print("--- Load Balancer Starting ---")

    # --- Step 1: Connect to all backend servers ---
    server_sockets = {}
    # This dictionary will track the estimated time when each server will be free.
    server_finish_times = {} 

    for name, config in SERVERS.items():
        try:
            print(f"Connecting to server: {name} at {config['host']}:{config['port']}...")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((config['host'], config['port']))
            sock.setblocking(False) # Use non-blocking sockets
            server_sockets[sock] = {'name': name, 'type': config['type']}
            server_finish_times[name] = time.time() # Initially, servers are free now.
            print(f"Successfully connected to {name}.")
        except socket.error as e:
            print(f"Error: Could not connect to server {name}. {e}")
            return # Exit if a server connection fails

    # --- Step 2: Set up the listening socket for clients ---
    listening_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listening_socket.setblocking(False)
    listening_socket.bind((LISTENING_HOST, LISTENING_PORT))
    listening_socket.listen(10) # Allow a backlog of 10 connections
    print(f"Load Balancer is listening on {LISTENING_HOST}:{LISTENING_PORT}")

    # --- Step 3: Initialize data structures for select() ---
    
    # Sockets we are reading from
    inputs = [listening_socket] + list(server_sockets.keys())
    
    # Sockets we are writing to (initially none)
    outputs = []

    # Map to link client sockets to their corresponding server sockets and vice-versa
    # This is key to routing responses back to the correct client.
    client_to_server = {}
    server_to_client = {}

    # --- Step 4: Main Event Loop using select() ---
    print("\n--- Entering main event loop ---\n")
    try:
        while True:
            # select() blocks until at least one socket is ready for I/O
            readable, writable, exceptional = select.select(inputs, outputs, inputs)

            # Handle readable sockets
            for sock in readable:
                if sock is listening_socket:
                    # A new client is trying to connect
                    client_conn, client_addr = sock.accept()
                    print(f"Accepted connection from client {client_addr}")
                    client_conn.setblocking(False)
                    inputs.append(client_conn)
                
                elif sock in server_sockets:
                    # Data received from a backend server (a response)
                    response = sock.recv(BUFFER_SIZE)
                    if response:
                        print(f"Received response from server {server_sockets[sock]['name']}: {response.decode()}")
                        # Find the client this response is for and send it
                        client_socket = server_to_client.pop(sock, None)
                        if client_socket:
                            client_socket.send(response)
                            # Clean up
                            inputs.remove(client_socket)
                            client_socket.close()
                            print(f"Relayed response to client and closed connection.")
                        else:
                            print("Warning: Received response from server but couldn't find matching client.")
                    else:
                        # Server closed connection
                        print(f"Server {server_sockets[sock]['name']} closed connection.")
                        inputs.remove(sock)
                        sock.close()

                else: # It's a client socket with a new request
                    request = sock.recv(BUFFER_SIZE)
                    if request:
                        request_str = request.decode().strip()
                        print(f"Received request from client: {request_str}")

                        # --- Scheduling Logic ---
                        best_server_name = None
                        earliest_finish_time = float('inf')

                        current_time = time.time()

                        for name, config in SERVERS.items():
                            # Find the server's socket
                            server_sock_obj = next((s for s, d in server_sockets.items() if d['name'] == name), None)
                            if not server_sock_obj: continue

                            # Calculate when the server would start this new job
                            # It's either now or when its last job finishes.
                            start_time = max(current_time, server_finish_times[name])
                            
                            processing_time = get_estimated_processing_time(config['type'], request_str)
                            
                            finish_time = start_time + processing_time

                            if finish_time < earliest_finish_time:
                                earliest_finish_time = finish_time
                                best_server_name = name
                        
                        # Update the chosen server's estimated finish time
                        server_finish_times[best_server_name] = earliest_finish_time
                        
                        # Find the socket for the chosen server
                        chosen_server_socket = next((s for s, d in server_sockets.items() if d['name'] == best_server_name), None)
                        
                        if chosen_server_socket:
                            print(f"Scheduling request '{request_str}' to server '{best_server_name}'. Estimated finish: {earliest_finish_time}")
                            # Forward the request
                            chosen_server_socket.send(request)
                            # Map sockets for response routing
                            client_to_server[sock] = chosen_server_socket
                            server_to_client[chosen_server_socket] = sock
                        else:
                            print(f"Error: Could not find socket for chosen server {best_server_name}")
                            inputs.remove(sock)
                            sock.close()

                    else:
                        # Client closed connection without sending data
                        print("Client disconnected.")
                        inputs.remove(sock)
                        sock.close()

            # Handle exceptional conditions
            for sock in exceptional:
                print(f"Handling exceptional condition for {sock.getpeername()}")
                inputs.remove(sock)
                if sock in outputs:
                    outputs.remove(sock)
                sock.close()

    except KeyboardInterrupt:
        print("\n--- Shutting down Load Balancer ---")
    finally:
        # Clean up all sockets
        for sock in inputs:
            sock.close()

if __name__ == "__main__":
    main()