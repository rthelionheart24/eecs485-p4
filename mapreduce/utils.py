"""Utils file.

This file is to house code common between the Manager and the Worker

"""

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
    