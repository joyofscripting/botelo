from abc import ABC, abstractmethod
from typing import Union, Optional, List
from botelo.structures import BoteloFilesBucket
import logging
logger = logging.getLogger(__name__)


class CleanerError(Exception):
    """Raised when a cleaner fails.

    Args:
        message (str): Human readable string describing the exception.
        cleaner (:obj:`AbstractCleaner`): An instance of AbstractCleaner who raised the exception.

    Attributes:
        message (str): Human readable string describing the exception.
        cleaner (:obj:`AbstractCleaner`): An instance of AbstractCleaner who raised the exception."""
    def __init__(self, message, cleaner):
        self.message = message
        self.cleaner = cleaner


class CleanerResult(object):
    """Contains the result of the cleaner.

    Args:
        result (obj): The result of the cleaner.

    Attributes:
        result (obj): The result of the cleaner.
    """
    def __init__(self, result):
        self.result = result


class AbstractCleaner(ABC):
    """This abstract class describes the interface that must be implemented by a cleaner.

    Every forwarder must implement a method named cleanup which returns a CleanerResult
    or a CleanerError in case of an exception.

    In addition every cleaner must have a property named name which returns its name.
    """
    @abstractmethod
    def cleanup(self, files_bucket: BoteloFilesBucket, shared_info_collection: dict) -> Union[CleanerResult, CleanerError]:
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError


class GeneralCleaner(AbstractCleaner):
    """Removes files created by an export job.

    Args:
        suffix_filter (Optional[List[str]]): Optional list of suffixes. Enables cleaning up only files with a certain suffix.
        name (Optional[str]): The name of this forwarder. Defaults to "General Cleaner".
    """
    def __init__(self, suffix_filter: Optional[List[str]] = None, name: Optional[str] = "General Cleaner"):
        self._name = name
        if suffix_filter is None:
            suffix_filter = []
        self._suffix_filter = suffix_filter

    @property
    def name(self) -> str:
        """Returns the name of the cleaner."""
        return self._name

    def cleanup(self, files_bucket: BoteloFilesBucket, shared_info_collection: dict) -> Union[CleanerResult, CleanerError]:
        """Removes files. That's it."""
        filepaths = files_bucket.get_all_filepaths()
        if self._suffix_filter:
            filepaths = [filepath for filepath in filepaths if filepath.suffix in self._suffix_filter]
        try:
            for filepath in filepaths:
                if filepath.exists():
                    logger.info("Removing file {0}".format(filepath))
                    filepath.unlink()
            return CleanerResult("Cleanup was successful")
        except Exception as e:
            logger.error('Removing files failed: {0}'.format(str(e)))
            return CleanerError(str(e), self)
