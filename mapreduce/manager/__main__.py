"""MapReduce framework Manager node."""
import json
import logging
import os
import pathlib
import sys
import threading
import time
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
        self.state = "free"
        self.remaining_messages = []
        self.num_messages_left = None
        self.signals = {"shutdown": False}
        self.dispatch = {
            "shutdown": self.shutdown,
            "register": self.register,
            "new_manager_job": self.new_manager_job,
            "finished": self.finished
        }
        self.udp_dispatch = {"heartbeat": self.heartbeat}
        self.tmp = pathlib.Path("tmp")
        self.current_task = None
        self.reduce_input_paths = []

        # Initialize Manager
        self.tmp.mkdir(parents=True, exist_ok=True)
        self.tmp.glob("job-*")
        self.listen()
        while not self.signals["shutdown"]:
            time.sleep(1)

    def listen(self):
        tcp = threading.Thread(
            target=mapreduce.utils.tcp_listen,
            args=(
                self.host,
                self.port,
                self.dispatch
            ),
        )
        udp = threading.Thread(
            target=mapreduce.utils.udp_listen,
            args=(
                self.host,
                self.hb_port,
                self.udp_dispatch,
                self.signals
            ),
        )
        fault_tolerance = threading.Thread(
            target=self.fault_tolerance_thread,
        )
        tcp.start()
        udp.start()
        fault_tolerance.start()

    def shutdown(self, message_dict):
        for worker in self.get_living_workers():
            mapreduce.utils.tcp_send_message(
                worker["host"], worker["port"], message_dict
            )
        self.signals["shutdown"] = True
        LOGGER.info("Initiate Shutdown")

    def register(self, message_dict):
        host, port = message_dict["worker_host"], message_dict["worker_port"]
        self.workers.append({"host": host,
                             "port": port,
                             "state": "ready",
                             "message": "",
                             "time_since_last_beat": 0})
        register_acknowledgement = {
            "message_type": "register_ack",
            "worker_host": host,
            "worker_port": port,
        }
        mapreduce.utils.tcp_send_message(host, port, register_acknowledgement)
        if not self.job_queue.empty() and len(self.workers) == 1:
            self.state = "map"
            self.assign_mapper_task(self.job_queue.get())

    def get_ready_workers(self):
        return [w for w in self.workers if w["state"] == "ready"]

    def get_living_workers(self):
        return [w for w in self.workers if w["state"] != "dead"]

    def assign_mapper_task(self, manager_task):
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
        self.num_messages_left = len(partitions)
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
                self.change_worker_message(worker_host, worker_port, message)
            else:
                self.remaining_messages.append(message)
        # Log map stage starts
        LOGGER.info("Manager:%s begin map stage", self.port)
        self.state = "map"

    def assign_reducer_task(self):
        workers = self.get_ready_workers()
        num_reducers = self.current_task.num_reducers
        partitions = [self.reduce_input_paths[i::num_reducers]
                      for i in range(num_reducers)]
        self.remaining_messages = []
        self.num_messages_left = len(partitions)
        for i, partition in enumerate(partitions):
            wi = i if i < len(workers) else 0
            worker_host, worker_port = workers[wi]["host"], workers[wi]["port"]
            message = {
                "message_type": "new_reduce_task",
                "task_id": i,
                "executable": self.current_task.reducer_executable,
                "input_paths": [_ for _ in partitions[i]],
                "output_directory": self.current_task.output_directory,
                "worker_host": worker_host,
                "worker_port": worker_port,
            }
            if i < len(workers):
                mapreduce.utils.tcp_send_message(worker_host, worker_port,
                                                 message)
                self.change_worker_state(worker_host, worker_port, "busy")
                self.change_worker_message(worker_host, worker_port, message)
            else:
                self.remaining_messages.append(message)
        LOGGER.info("Manager:%s begin reduce stage", self.port)

    def get_worker_index(self, worker_host, worker_port):
        for i, worker in enumerate(self.workers):
            if worker["host"] == worker_host and worker["port"] == worker_port:
                return i

    def change_worker_state(self, worker_host, worker_port, new_state):
        index = self.get_worker_index(worker_host, worker_port)
        self.workers[index]["state"] = new_state

    def change_worker_message(self, worker_host, worker_port, new_message):
        index = self.get_worker_index(worker_host, worker_port)
        self.workers[index]["message"] = new_message

    def finished(self, message_dict):
        self.change_worker_state(
            message_dict["worker_host"], message_dict["worker_port"], "ready"
        )
        if self.state == "map":
            self.reduce_input_paths.extend(message_dict["output_paths"])
        self.num_messages_left -= 1
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
            self.change_worker_message(worker_host, worker_port, message)
            self.change_worker_state(worker_host, worker_port, "busy")
        elif self.num_messages_left != 0:
            return
        elif self.state == "map":
            LOGGER.info("Manager:%s end map stage", self.port)
            self.state = "reduce"
            self.assign_reducer_task()
        elif self.state == "reduce":
            LOGGER.info("Manager:%s end reduce stage", self.port)
            self.state = "free"
            self.reduce_input_paths = None
            self.current_task = None
            if self.get_ready_workers() and not self.job_queue.empty():
                self.state = "map"
                self.assign_mapper_task(self.job_queue.get())

    def reassign_job(self):
        print("reassigning dead worker's task")
        workers = self.get_ready_workers()
        if self.remaining_messages and workers:
            message = self.remaining_messages.pop(0)
            host, port = workers[0]["host"], workers[0]["port"]
            message["worker_host"] = host
            message["worker_port"] = port
            mapreduce.utils.tcp_send_message(host, port, message)
            self.change_worker_message(host, port, message)
            self.change_worker_state(host, port, "busy")

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
        pathlib.Path(output_directory).mkdir(parents=True, exist_ok=True)
        self.job_counter += 1

        manager_task = mapreduce.utils.ManagerTask(
            input_directory,
            output_directory,
            temp_intermediate_dir,
            mapper_executable,
            reducer_executable,
            num_mappers,
            num_reducers,
        )
        self.current_task = manager_task
        if self.get_ready_workers() and self.state == "free":
            self.state = "map"
            self.assign_mapper_task(manager_task)
        else:
            self.job_queue.put(manager_task)

    def heartbeat(self, message_dict):
        host, port = message_dict["worker_host"], message_dict["worker_port"]
        for worker in self.workers:
            if worker["host"] == host and worker["port"] == port:
                worker["time_since_last_beat"] = 0
                return

    def fault_tolerance_thread(self):
        while not self.signals["shutdown"]:
            for worker in self.get_living_workers():
                # TODO: When should i mark dead
                if worker["time_since_last_beat"] >= 10:
                    worker["state"] = "dead"
                    self.remaining_messages.append(worker["message"])
                    self.reassign_job()
                else:
                    worker["time_since_last_beat"] += 1
            time.sleep(1)


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
