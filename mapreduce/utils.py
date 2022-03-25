"""Utils file.

This file is to house code common between the Manager and the Worker

"""
import json
import socket


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
            print("message from", address[0], ": ", message_dict[
                "message_type"])
            message_type = message_dict["message_type"]
            if message_type in dispatch:
                dispatch[message_type](message_dict)


def udp_listen(host, port, dispatch, signals):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

        # Bind the UDP socket to the server
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.settimeout(1)

        # No sock.listen() since UDP doesn't establish connections like TCP

        # Receive incoming UDP messages
        while not signals["shutdown"]:
            try:
                message_bytes = sock.recv(4096)
            except socket.timeout:
                continue
            message_str = message_bytes.decode("utf-8")
            message_dict = json.loads(message_str)
            message_type = message_dict["message_type"]
            if message_type in dispatch:
                print(message_type)
                dispatch[message_type](message_dict)


def tcp_send_message(host, port, message):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))
        message = json.dumps(message)
        sock.sendall(message.encode('utf-8'))


def udp_send_message(host, port, message):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
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


class ReduceTask:
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


class ManagerTask:
    def __init__(self, input_directory, output_directory,
                 intermediate_directory, mapper_executable,
                 reducer_executable, num_mappers, num_reducers):
        self.input_directory = input_directory
        self.output_directory = output_directory
        self.intermediate_directory = intermediate_directory
        self.mapper_executable = mapper_executable
        self.reducer_executable = reducer_executable
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.finished = False

    def __repr__(self):
        """Return a string representation of the object."""
        return (
            "MapTask("
            f"input_files={repr(self.input_files)}, "
            f"output_directory={repr(self.output_directory)}"
            f"mapper_executable={repr(self.mapper_executable)}, "
            f"reducer_executable={repr(self.reducer_executable)}"
            f"num_mappers={repr(self.num_mappers)}, "
            f"num_reducer={repr(self.num_reducers)}"
            ")"
        )
