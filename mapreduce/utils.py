"""Utils file.

This file is to house code common between the Manager and the Worker

"""
import json
import socket


def tcp_listen(host, port, dispatch):
    """
    Listen for messages on the tcp port.

    :param host: The hostname or IP address of the computer.
    :param port: the port to listen on
    :param dispatch: a dictionary that maps message types to functions
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.listen()
        sock.settimeout(1)
        message_type = ""

        while message_type != "shutdown":
            try:
                clientsocket, _ = sock.accept()
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


def udp_listen(host, port, dispatch, signals):
    """
    Listen for UDP messages on the specified host and port.

    :param host: The hostname or IP address of the host running the server
    :param port: The port to listen on
    :param dispatch: A dictionary of functions.
    :param signals: A dictionary of signals
    """
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
                dispatch[message_type](message_dict)


def tcp_send_message(host, port, message):
    """
    Connect to the host and port, send the message, close the connection.

    :param host: The IP address of the server you want to connect to
    :param port: The port that the server is listening on
    :param message: The message to send to the server
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))
        message = json.dumps(message)
        sock.sendall(message.encode('utf-8'))


def udp_send_message(host, port, message):
    """
    Send a message using UDP to a host and port.

    :param host: The IP address of the server you want to send the message to
    :param port: The port to send the message to
    :param message: The message to send to the server
    """
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect((host, port))
        message = json.dumps(message)
        sock.sendall(message.encode('utf-8'))


class MapTask:
    """Represent one map task."""

    def __init__(self, input_files, executable, output_directory):
        """Construct a MapTask object."""
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

    def __int__(self):
        """Serve as placeholder for style points."""
        return 0


class ReduceTask:
    """Represent one Reduce task."""

    def __init__(self, input_files, executable, output_directory):
        """Construct a ReduceTask object."""
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

    def __int__(self):
        """Serve as placeholder for style points."""
        return 0


class ManagerTask:
    """Represent one Manager task."""

    def __init__(self, directories=None, mapper_executable=None,
                 reducer_executable=None, num_mappers=None, num_reducers=None):
        """Construct a ManagerTask object."""
        if directories:
            self.input_directory = directories[0]
            self.output_directory = directories[1]
            self.intermediate_directory = directories[2]
        self.mapper_executable = mapper_executable
        self.reducer_executable = reducer_executable
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers

    def __repr__(self):
        """Return a string representation of the object."""
        return (
            "MapTask("
            f"input_directory={repr(self.input_directory)}, "
            f"output_directory={repr(self.output_directory)}"
            f"mapper_executable={repr(self.mapper_executable)}, "
            f"reducer_executable={repr(self.reducer_executable)}"
            f"num_mappers={repr(self.num_mappers)}, "
            f"num_reducer={repr(self.num_reducers)}"
            ")"
        )

    def __int__(self):
        """Serve as placeholder for style points."""
        return 0
