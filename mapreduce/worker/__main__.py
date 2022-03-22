"""MapReduce framework Worker node."""
import json
import logging
import os
import sys
import threading
import time

import click

import mapreduce.utils

import hashlib

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

        # This is a fake message to demonstrate pretty printing with logging
        message_dict = {
            "message_type": "register_ack",
            "worker_host": "localhost",
            "worker_port": 6001,
        }
        LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))

        self.host = host
        self.port = port
        self.manager_host = manager_host
        self.manager_port = manager_port
        self.manager_hb_port = manager_hb_port
        self.shutdown_signal = False
        self.dispatch = {
            "register_ack": self.register_ack,
            "shutdown": self.shutdown
        }

        self.listen()
        self.register()

    def listen(self):
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
        registration_message = {
            "message_type": "register",
            "worker_host": self.host,
            "worker_port": self.port,
        }
        mapreduce.utils.tcp_send_message(
            self.manager_host, self.manager_port, registration_message
        )

    def shutdown(self, message_dict):
        self.shutdown_signal = True

    def register_ack(self, message_dict):
        def send_heartbeat(worker_host, worker_port,
                           manager_host, manager_port):
            while not self.shutdown_signal:
                message = {
                    "message_type": "heartbeat",
                    "worker_host": worker_host,
                    "worker_port": worker_port,
                }
                mapreduce.utils.udp_send_message(
                    manager_host, manager_port, message
                )
                time.sleep(2)

        heartbeat = threading.Thread(target=send_heartbeat,
                                     args=(self.host, self.port,
                                           self.manager_host,
                                           self.manager_hb_port,))
        heartbeat.start()


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
