import threading
import time
import socket
import uuid
import json
import sys

BUFFER_SIZE = 1024

# Message Class
class Message:   
    def __init__(self, uuid_val, flag):
        self.uuid = uuid_val # Sender's UUID
        self.flag = flag # Starts as 0, leader unknown

    def to_json(self):
        message = {
            "uuid": str(self.uuid),
            "flag": self.flag
        }

        return json.dumps(message)
    
    @staticmethod
    def from_json(text):
        message = json.loads(text)

        return Message(
            uuid.UUID(message["uuid"]),
            message["flag"]
        )

#------------------------------------------------------------------------------------------

# Function to read config.txt
def read_config():
    with open("config.txt", "r") as config_file:
        lines = config_file.readlines()

    server_host, server_port = lines[0].strip().split(",")
    client_host, client_port = lines[1].strip().split(",")

    return (
        server_host,
        int(server_port),
        client_host,
        int(client_port)
    )

#------------------------------------------------------------------------------------------

# Server: wait for message from right edge
def server_function(connections, ready, host, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
       
       server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
       server_sock.bind((host, port))
       server_sock.listen(1)

       print(f"[TCP Server] Listening on port {port} ...")

       # Wait for right edge
       connections["incoming"], address = server_sock.accept()

       print(f"Connected by {address}")
       ready.set()

#------------------------------------------------------------------------------------------

# Client
def connect_to_next_node(host, port):
    while True:
        client_sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)

        try:
            client_sock.connect((host, port))
            print(f"Connected to {host}:{port}")
            return client_sock

        except ConnectionRefusedError:
            client_sock.close()
            time.sleep(1)

#------------------------------------------------------------------------------------------

def write_log(log_file, message):
    print(message)
    log_file.write(message + "\n")
    log_file.flush()

#------------------------------------------------------------------------------------------

def main():

    server_host, server_port, client_host, client_port = read_config()

    # Generate id for this process 
    process_uuid = uuid.uuid4()

    # We start not knowing leader
    leader_id = None
    state = 0


    log_name = sys.argv[1] if len(sys.argv) > 1 else "log.txt"
    log_file = open(log_name, "w") # open log file for writing

    write_log(log_file, f"Started with uuid={process_uuid}") #update log

    connections = {} # dictionary to hold sockets
    ready = threading.Event() # for communication between threads

    # the server runs in another thread because accept() blocks
    server_thread = threading.Thread(
        target=server_function,
        args=(connections, ready, server_host, server_port),
        daemon=True
    )

    server_thread.start()

    input("Press Enter when everyone is ready.\n")

    time.sleep(1)

    # connect to the next node in the ring [client portion]
    outgoing = connect_to_next_node(client_host, client_port)

    # wait until server accepted the previous node
    ready.wait()
    incoming = connections["incoming"]

    # send this process's uuid when it starts the election
    first_message = Message(process_uuid, 0)
    outgoing.sendall(first_message.to_json().encode())

    write_log(
        log_file,
        f"Sent: uuid={first_message.uuid}, flag={first_message.flag}"
    )

    message_buffer = ""
    done = False
    while  not done:
        data = incoming.recv(BUFFER_SIZE)

        if not data:
            break

        message_buffer = message_buffer + data.decode()

        # use a while loop instead of an if statement incase the buffer contains multiple messages
        while "}" in message_buffer and not done:

            # find the first }
            bracket_pos = message_buffer.find("}") + 1

            # separate the first message (if there's multiple)
            message_text = message_buffer[:bracket_pos]

            # update the buffer so a potential 2nd message can be processed in this loop
            message_buffer = message_buffer[bracket_pos:]

            message = Message.from_json(message_text)

            if message.uuid > process_uuid:
                comparison = "greater"
            elif message.uuid == process_uuid:
                comparison = "same"
            else:
                comparison = "less"

            write_log(
                log_file,
                f"Received: uuid={message.uuid}, "
                f"flag={message.flag}, "
                f"{comparison}, "
                f"state={state}"
            )

            if message.flag == 1: # incoming message specifies leader
                state = 1
                # record leader
                leader_id = message.uuid

                write_log(
                    log_file,
                    f"Leader is decided to {leader_id}"
                )

                if message.uuid == process_uuid: # i am the leader
                    # we're done
                    done = True
                else:
                    # i am not the leader, forward the leader message to next node
                    outgoing.sendall(message.to_json().encode())

                    write_log(
                        log_file,
                        f"Sent: uuid={message.uuid}, flag={message.flag}"
                    )
                    done = True

            elif message.uuid > process_uuid: # potential leader, forward message
                # forward larger UUIDs
                outgoing.sendall(message.to_json().encode())

                write_log(
                    log_file,
                    f"Sent: uuid={message.uuid}, flag={message.flag}"
                )

            elif message.uuid == process_uuid: # i am the leader
                # record myself as leader
                state = 1
                leader_id = process_uuid

                write_log(
                    log_file,
                    f"Leader is decided to {leader_id}"
                )

                # send leader message
                leader_message = Message(process_uuid, 1)
                outgoing.sendall(leader_message.to_json().encode())

                write_log(
                    log_file,
                    f"Sent: uuid={leader_message.uuid}, flag={leader_message.flag}"
                )

            else:
                # Ignore smaller UUIDs.
                write_log(log_file, "Ignored message")

    print(f"leader is {leader_id}")
    log_file.close()


if __name__ == "__main__":
    main()