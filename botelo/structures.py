from pathlib import Path
from typing import List
import logging
logger = logging.getLogger(__name__)


class BoteloFilesBucket(object):
    """Keeps together all the filepaths that are created during the execution of a single export job."""
    def __init__(self):
        self._exported_files = []
        self._query_file = None
        self._finished_files = []
        self._package_files = []

    @property
    def exported_files(self) -> List[Path]:
        """Returns the list of filepaths of exported files."""
        return self._exported_files

    @exported_files.setter
    def exported_files(self, exported_files: List[Path]):
        self._exported_files = exported_files

    @property
    def query_file(self) -> Path:
        """Returns the path to the file containing the saved query."""
        return self._query_file

    @query_file.setter
    def query_file(self, query_file: Path):
        self._query_file = query_file

    @property
    def finished_files(self) -> List[Path]:
        """Returns the list of filepaths of finished files."""
        return self._finished_files

    @finished_files.setter
    def finished_files(self, finished_files: List[Path]):
        self._finished_files = finished_files

    @property
    def package_files(self) -> List[Path]:
        """Returns the list of filepaths of package files."""
        return self._package_files

    @package_files.setter
    def package_files(self, package_files: List[Path]):
        self._package_files = package_files

    def add_export_filepath(self, filepath: Path):
        """Adds a filepath to the list of exported files."""
        self._exported_files.append(filepath)

    def add_finished_filepath(self, filepath: Path):
        """Adds a filepath to the list of finished files."""
        self._finished_files.append(filepath)

    def add_package_filepath(self, filepath: Path):
        """Adds a filepath to the list of package files."""
        self._package_files.append(filepath)

    def get_relevant_files(self) -> List[Path]:
        """Returns the current unique list of relevant filepaths."""
        relevant_filepaths = []
        if self.package_files:
            relevant_filepaths.extend(self.package_files)
        elif self.finished_files:
            relevant_filepaths.extend(self.finished_files)
        else:
            relevant_filepaths.extend(self.exported_files)

        if self.query_file:
            relevant_filepaths.append(self.query_file)

        return list(set(relevant_filepaths))

    def get_all_filepaths(self) -> List[Path]:
        """Returns the current unique list of all filepaths created."""
        filepaths = []

        filepaths.extend(self.exported_files)
        filepaths.extend(self.finished_files)
        filepaths.extend(self.package_files)
        if self.query_file:
            filepaths.append(self.query_file)

        return list(set(filepaths))

    def log_filepaths(self):
        logger.info("Created export files: {0}".format(len(self.exported_files)))
        logger.info("Finished files: {0}".format(len(self.finished_files)))
        logger.info("Package files: {0}".format(len(self.package_files)))


class BoteloProcessBadge(object):
    """"""
    def __init__(self, identifier: str, process_number: int, number_of_processes: int):
        self._identifier = identifier
        self._process_number = process_number
        self._number_of_processes = number_of_processes

    @property
    def identifier(self):
        return self._identifier

    @property
    def process_number(self):
        return self._process_number

    @property
    def number_of_processes(self):
        return self._number_of_processes
