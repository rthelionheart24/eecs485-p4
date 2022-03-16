"""Utils file.

This file is to house code common between the Manager and the Worker

"""
import socket
import json


def tcp_listen(host, port, dispatch):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.listen()
        sock.settimeout(1)
        message_type = ""

        while message_type != "shutdown":
            try:
                clientsocket, address = sock.accept()
            except socket.timeout:
                continue

            with clientsocket:
                message_chunks = []
                while True:
                    try:
                        data = clientsocket.recv(4096)
                    except socket.timeout:
                        continue
                    if not data:
                        break
                    message_chunks.append(data)

            message_bytes = b''.join(message_chunks)
            message_str = message_bytes.decode("utf-8")

            try:
                message_dict = json.loads(message_str)
            except json.JSONDecodeError:
                continue
            message_type = message_dict["message_type"]
            if message_type in dispatch:
                dispatch[message_type](message_dict)


def udp_listen(host, port):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.settimeout(1)

        while True:
            try:
                message_bytes = sock.recv(4096)
            except socket.timeout:
                continue
            message_str = message_bytes.decode("utf-8")
            message_dict = json.loads(message_str)
            print(message_dict)


def send_message(host, port, message):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))
        message = json.dumps(message)
        sock.sendall(message.encode('utf-8'))


class MapTask:
    """Represent one map task."""

    def __init__(self, input_files, executable, output_directory):
        self.input_files = input_files
        self.executable = executable
        self.output_directory = output_directory

    def __repr__(self):
        """Return a string representation of the object."""
        return (
            "MapTask("
            f"input_files={repr(self.input_files)}, "
            f"executable={repr(self.executable)}, "
            f"output_directory={repr(self.output_directory)}"
            ")"
        )
