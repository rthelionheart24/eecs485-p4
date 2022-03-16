"""MapReduce framework Manager node."""
from email import message
import sys
import os
import logging
import json
import time
import click
import pathlib
import threading
import socket
import mapreduce.utils

# Configure logging
LOGGER = logging.getLogger(__name__)


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port, hb_port):
        """Construct a Manager instance and start listening for messages."""

        LOGGER.info(
            "Starting manager host=%s port=%s hb_port=%s pwd=%s",
            host, port, hb_port, os.getcwd(),
        )

        # This is a fake message to demonstrate pretty printing with logging
        message_dict = {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": 6001,
        }
        LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))

        self.host = host
        self.port = port
        self.hb_port = hb_port
        self.living_workers = []
        self.signals = {'shutdown': False}
        self.dispatch = {
            "shutdown": self.shutdown,
            "register": self.register
        }

        tmp = pathlib.Path(os.getcwd()) / "tmp"
        tmp.mkdir(parents=True, exist_ok=True)
        tmp.glob("job-*")

        self.listen()

    def listen(self):
        tcp = threading.Thread(target=mapreduce.utils.tcp_listen,
                               args=(self.host, self.port, self.dispatch,))
        udp = threading.Thread(target=mapreduce.utils.udp_listen,
                               args=(self.host, self.hb_port, self.signals,))
        tcp.start()
        udp.start()

    def shutdown(self, message_dict):
        for living_worker in self.living_workers:
            mapreduce.utils.tcp_send_message(living_worker["host"],
                                             living_worker["port"],
                                             message_dict)
        self.signals['shutdown'] = True

    def register(self, message_dict):
        host, port = message_dict["worker_host"], message_dict["worker_port"]
        self.living_workers.append({"host": host, "port": port})
        register_acknowledgement = {
            "message_type": "register_ack",
            "worker_host": host,
            "worker_port": port
        }
        mapreduce.utils.tcp_send_message(host, port, register_acknowledgement)
        # TODO: Check the job queue to see if any work can be assigned once
        #  the first worker registered


def fault_tolerance_thread(self):
    pass


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--hb-port", "hb_port", default=5999)
def main(host, port, hb_port):
    """Run Manager."""
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)
    Manager(host, port, hb_port)


if __name__ == '__main__':
    main()
