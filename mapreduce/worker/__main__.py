"""MapReduce framework Worker node."""
import contextlib
import hashlib
import heapq
import logging
import os
import subprocess
import sys
import threading
import time

import click

import mapreduce.utils

# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a worker node in a MapReduce cluster."""

    def __init__(self, host, port, manager_host, manager_port,
                 manager_hb_port):
        """Construct a Worker instance and start listening for messages."""
        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host,
            port,
            os.getcwd(),
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s manager_hb_port=%s",
            manager_host,
            manager_port,
            manager_hb_port,
        )

        self.host = host
        self.port = port
        self.manager_host = manager_host
        self.manager_port = manager_port
        self.manager_hb_port = manager_hb_port
        self.shutdown_signal = False
        self.dispatch = {
            "register_ack": self.register_ack,
            "shutdown": self.shutdown,
            "new_map_task": self.map,
            "new_reduce_task": self.reduce
        }

        self.listen()
        self.register()
        while not self.shutdown_signal:
            time.sleep(1)

    def listen(self):
        """Start a TCP listener on the host and port specified in the class."""
        tcp = threading.Thread(
            target=mapreduce.utils.tcp_listen,
            args=(
                self.host,
                self.port,
                self.dispatch,
            ),
        )
        tcp.start()

    def register(self):
        """Send a registration message to the manager."""
        registration_message = {
            "message_type": "register",
            "worker_host": self.host,
            "worker_port": self.port,
        }
        mapreduce.utils.tcp_send_message(
            self.manager_host, self.manager_port, registration_message
        )

    def shutdown(self, _):
        """Set the shutdown_signal to True."""
        self.shutdown_signal = True

    def register_ack(self, _):
        """Send a heartbeat to the manager every 2 seconds."""

        def send_heartbeat(worker_host, worker_port, manager_host,
                           manager_port):
            while not self.shutdown_signal:
                message = {
                    "message_type": "heartbeat",
                    "worker_host": worker_host,
                    "worker_port": worker_port,
                }
                mapreduce.utils.udp_send_message(manager_host, manager_port,
                                                 message)
                time.sleep(2)

        heartbeat = threading.Thread(
            target=send_heartbeat,
            args=(
                self.host,
                self.port,
                self.manager_host,
                self.manager_hb_port,
            ),
        )
        heartbeat.start()

    def map(self, message_dict):
        """
        Run the map task.

        :param message_dict: a dictionary containing the message data
        """
        task_id = message_dict["task_id"]
        output_paths = []
        num_partitions = message_dict["num_partitions"]
        for i in range(num_partitions):
            intermediate_file_name = (
                f"{message_dict['output_directory']}/maptask"
                f"{task_id:05}-part{i:05}"
            )
            output_paths.append(intermediate_file_name)
        with contextlib.ExitStack() as stack:
            output_files = [
                stack.enter_context(open(_, "a", encoding="utf-8"))
                for _ in output_paths
            ]
            for input_path in message_dict["input_paths"]:
                with open(input_path, encoding="utf-8") as infile:
                    with subprocess.Popen(
                            message_dict["executable"],
                            stdin=infile,
                            stdout=subprocess.PIPE,
                            universal_newlines=True,
                    ) as map_process:
                        for line in map_process.stdout:
                            # Add line to correct partition output file
                            partition = int(
                                hashlib.md5(line.split("\t")[0].encode(
                                    "utf-8")).hexdigest(),
                                base=16) % num_partitions
                            output_files[partition].write(line)
        self.send_finished_message(task_id, output_paths)

    def reduce(self, message_dict):
        """
        Run a reduce task.

        :param message_dict: The message dictionary
        """
        # Sort each input file by line
        for input_path in message_dict["input_paths"]:
            with open(input_path, "r", encoding="utf-8") as file:
                data = sorted(file.readlines())
            with open(input_path, "w", encoding="utf-8") as file:
                file.writelines(data)
        task_id = message_dict["task_id"]
        output_path = (
            f"{message_dict['output_directory']}/"
            f"part-{task_id:05}"
        )
        # Run the reduce executable on each line of input across all files,
        # sorted by line, and write all output to a single file.
        with contextlib.ExitStack() as stack:
            files = [
                stack.enter_context(open(_, encoding="utf-8"))
                for _ in message_dict["input_paths"]
            ]
            infile = heapq.merge(*files)
            with open(output_path, "a", encoding="utf-8") as outfile:
                with subprocess.Popen(
                        message_dict["executable"],
                        universal_newlines=True,
                        stdin=subprocess.PIPE,
                        stdout=outfile,
                ) as reduce_process:
                    # Pipe input to reduce_process
                    for line in infile:
                        reduce_process.stdin.write(line)
        self.send_finished_message(task_id, [output_path])

    def send_finished_message(self, task_id, output_paths):
        """
        Send a message to the manager that a task has finished.

        :param task_id: The task_id of the task that was just finished
        :param output_paths: The output paths of the task
        """
        message = {
            "message_type": "finished",
            "task_id": task_id,
            "output_paths": output_paths,
            "worker_host": self.host,
            "worker_port": self.port,
        }
        mapreduce.utils.tcp_send_message(
            self.manager_host, self.manager_port, message
        )


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--manager-hb-port", "manager_hb_port", default=5999)
def main(host, port, manager_host, manager_port, manager_hb_port):
    """Run Worker."""
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)
    Worker(host, port, manager_host, manager_port, manager_hb_port)


if __name__ == "__main__":
    main()
