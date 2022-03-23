"""MapReduce framework Manager node."""
import json
import logging
import os
import pathlib
import sys
import threading
from queue import Queue

import click

import mapreduce.utils

# Configure logging
LOGGER = logging.getLogger(__name__)


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port, hb_port):
        """Construct a Manager instance and start listening for messages."""

        LOGGER.info(
            "Starting manager host=%s port=%s hb_port=%s pwd=%s",
            host,
            port,
            hb_port,
            os.getcwd(),
        )

        # This is a fake message to demonstrate pretty printing with logging
        message_dict = {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": 6001,
        }
        LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))

        # Member Variables
        self.host = host
        self.port = port
        self.hb_port = hb_port
        self.job_counter = 0
        self.job_queue = Queue()
        self.workers = []
        self.is_free = True
        self.remaining_messages = []
        self.signals = {"shutdown": False}
        self.dispatch = {
            "shutdown": self.shutdown,
            "register": self.register,
            "new_manager_job": self.new_manager_job,
            "finished": self.finished
        }
        self.tmp = pathlib.Path("tmp")

        # Initialize Manager
        self.tmp.mkdir(parents=True, exist_ok=True)
        self.tmp.glob("job-*")
        self.listen()

    def listen(self):
        tcp = threading.Thread(
            target=mapreduce.utils.tcp_listen,
            args=(
                self.host,
                self.port,
                self.dispatch,
            ),
        )
        udp = threading.Thread(
            target=mapreduce.utils.udp_listen,
            args=(
                self.host,
                self.hb_port,
                self.signals,
            ),
        )
        tcp.start()
        udp.start()

    def shutdown(self, message_dict):
        for worker in [worker for worker in self.workers if
                       worker["state"] != "dead"]:
            mapreduce.utils.tcp_send_message(
                worker["host"], worker["port"], message_dict
            )
        self.signals["shutdown"] = True
        LOGGER.info("Initiate Shutdown")

    def register(self, message_dict):
        host, port = message_dict["worker_host"], message_dict["worker_port"]
        self.workers.append({"host": host, "port": port, "state": "ready"})
        register_acknowledgement = {
            "message_type": "register_ack",
            "worker_host": host,
            "worker_port": port,
        }
        mapreduce.utils.tcp_send_message(host, port, register_acknowledgement)
        # TODO: Check the job queue to see if any work can be assigned once
        #  the first worker registered

    def get_ready_workers(self):
        return [w for w in self.workers if w["state"] == "ready"]

    # TODO assign work to workers
    def assign_task(self, manager_task):
        workers = self.get_ready_workers()

        # Scan and sort the input directory by name
        files = []
        for entry in os.scandir(manager_task.input_directory):
            if entry.is_file():
                files.append(entry.name)
        files.sort()
        num_mappers = manager_task.num_mappers
        partitions = [files[i::num_mappers] for i in range(num_mappers)]

        # For each partition, construct a message and send to workers using
        # TCP When there are more files than workers, we give each worker a
        # job and reserve the remaining
        self.remaining_messages = []

        # Assign each worker a job
        for i, partition in enumerate(partitions):
            wi = i if i < len(workers) else 0
            worker_host, worker_port = workers[wi]["host"], workers[wi]["port"]
            message = {
                "message_type": "new_map_task",
                "task_id": i,
                "input_paths": [
                    f"{manager_task.input_directory}/{job}" for job in
                    partitions[i]
                ],
                "executable": manager_task.mapper_executable,
                "output_directory": str(manager_task.intermediate_directory),
                "num_partitions": manager_task.num_reducers,
                "worker_host": worker_host,
                "worker_port": worker_port,
            }
            if i < len(workers):
                mapreduce.utils.tcp_send_message(worker_host, worker_port,
                                                 message)
                self.change_worker_state(worker_host, worker_port, "busy")
            else:
                self.remaining_messages.append(message)
        # Log map stage starts
        LOGGER.info("Manager:%s begin map stage", self.port)
        self.is_free = False

    def change_worker_state(self, worker_host, worker_port, new_state):
        for worker in self.workers:
            if worker["host"] == worker_host and worker["port"] == worker_port:
                worker["state"] = new_state
                return

    def finished(self, message_dict):
        self.change_worker_state(
            message_dict["worker_host"], message_dict["worker_port"], "ready"
        )
        # If there are partitions left
        if len(self.remaining_messages) > 0:
            message = self.remaining_messages.pop(0)
            worker_host, worker_port = (
                message_dict["worker_host"],
                message_dict["worker_port"],
            )
            message["worker_host"] = worker_host
            message["worker_port"] = worker_port
            mapreduce.utils.tcp_send_message(worker_host, worker_port, message)
            self.change_worker_state(worker_host, worker_port, "busy")
        # Else
        else:
            LOGGER.info("Manager:%s end map stage", self.port)

            # TODO: start reduce stage

    def new_manager_job(self, message_dict):
        input_directory = message_dict["input_directory"]
        output_directory = message_dict["output_directory"]
        mapper_executable = message_dict["mapper_executable"]
        reducer_executable = message_dict["reducer_executable"]
        num_mappers = message_dict["num_mappers"]
        num_reducers = message_dict["num_reducers"]
        temp_intermediate_dir = \
            self.tmp / f"job-{self.job_counter}" / "intermediate"
        temp_intermediate_dir.mkdir(parents=True, exist_ok=True)
        self.job_counter += 1

        manager_task = mapreduce.utils.ManagerTask(
            input_directory,
            # TODO: what is the output directory here?
            output_directory,
            temp_intermediate_dir,
            mapper_executable,
            reducer_executable,
            num_mappers,
            num_reducers,
        )
        if self.get_ready_workers() and self.is_free:
            self.is_free = False
            self.assign_task(manager_task)
        else:
            self.job_queue.put(manager_task)


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
        f"Manager:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)
    Manager(host, port, hb_port)


if __name__ == "__main__":
    main()
