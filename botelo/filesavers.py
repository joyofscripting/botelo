from pathlib import Path
from abc import ABC, abstractmethod
import logging
from typing import Union

logger = logging.getLogger(__name__)


class FileSaverError(Exception):
    """Raised when a file saver fails."""
    def __init__(self, message, file_saver):
        self.message = message
        self.file_saver = file_saver


class FileSaverResult(object):
    def __init__(self, filepath):
        self.filepath = filepath


class AbstractTextSaver(ABC):

    @abstractmethod
    def save(self, shared_info_collection: dict) -> Union[FileSaverResult, FileSaverError]:
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self):
        raise NotImplementedError


class QuerySaver(AbstractTextSaver):

    def __init__(self, suffix, name="Query Saver"):
        super().__init__()
        self._name = name
        self.suffix = suffix

    @property
    def name(self):
        return self._name

    def save(self, shared_info_collection):
        try:
            if 'BT_QUERY' not in shared_info_collection.keys():
                raise KeyError("You must provide the query with the key 'BT_QUERY' in the shared´info collection.")

            query = shared_info_collection['BT_QUERY']
            filepath = self._get_filepath(shared_info_collection)
            logger.info("Saving query to text file {0}...".format(filepath))
            with open(filepath, mode='w', encoding='utf-8') as sql_file:
                sql_file.write(query)
            logger.info("Successfully saved the query")
            return FileSaverResult(filepath)
        except Exception as e:
            logger.error("Query export failed: {0}".format(str(e)))
            return FileSaverError(str(e), self)

    def _get_filepath(self, shared_info_collection):
        filename = "{0}".format(shared_info_collection['BT_FILE_BASENAME'])
        filepath = Path(shared_info_collection['BT_EXPORT_FOLDERPATH'], filename).with_suffix(self.suffix)
        return filepath
