from pathlib import Path
from abc import ABC, abstractmethod
from typing import Union, List, Optional
import zipfile
import tarfile
from botelo.structures import BoteloProcessBadge
import logging
logger = logging.getLogger(__name__)


class FilePackagerError(Exception):
    """Raised when a file packager fails.

    Args:
        message (str): Human readable string describing the exception.
        file_packager (:obj:`AbstractFilePackager`): An instance of AbstractFilePackager who raised the exception.

    Attributes:
        message (str): Human readable string describing the exception.
        file_packager (:obj:`AbstractFilePackager`): An instance of AbstractFilePackager who raised the exception."""
    def __init__(self, message, file_packager):
        self.message = message
        self.file_packager = file_packager


class FilePackagerResult(object):
    """Contains the result of the file packager.

    Args:
        filepaths (:obj:`Path`): A list of filepaths of the created archives.

    Attributes:
        filepaths (:obj:`Path`): A list of filepaths of the created archives.
    """
    def __init__(self, filepaths: List[Path]):
        self.filepaths = filepaths


class AbstractFilePackager(ABC):
    """This abstract class describes the interface that must be implemented by a file packager.

    Every file packager must implement a method named package_files which returns a FilePackagerResult
    or a FilePackagerError in case of an exception.

    In addition every file packager must have a property named name which returns its name.
    """
    @abstractmethod
    def package_files(self, filepaths: List[Path], shared_info_collection: dict, process_badge: BoteloProcessBadge) -> Union[FilePackagerResult, FilePackagerError]:
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError


class ZipFilePackager(AbstractFilePackager):
    """A file packager that creates a ZIP archive from several files.

    Args:
        name (Optional[str]): The name of this file packager. Defaults to "ZIP Archiver".
        overwrite_file (bool): May an existing file  be overwritten? Defaults to False.
    """
    def __init__(self, name: Optional[str] = "ZIP Archiver", suffix_filter: Optional[List[str]] = None, overwrite_file: bool = False):
        super().__init__()
        self._name = name
        self.suffix = '.zip'
        if suffix_filter is None:
            suffix_filter = []
        self.suffix_filter = suffix_filter
        self.overwrite_file = overwrite_file

    @property
    def name(self) -> str:
        """Returns the name of the file packager."""
        return self._name

    def package_files(self, filepaths: List[Path], shared_info_collection: dict, process_badge: BoteloProcessBadge) -> Union[FilePackagerResult, FilePackagerError]:
        """Creates a ZIP archive from the given filepaths."""
        try:
            logger.info("{0} Creating ZIP archive...".format(process_badge.identifier))
            zip_filepath = self._get_filepath(shared_info_collection)
            if not self.overwrite_file and zip_filepath.exists():
                raise FileExistsError("The filepath already exists: {0}".format(zip_filepath))
            if self.suffix_filter:
                filepaths = [filepath for filepath in filepaths if filepath.suffix in self.suffix_filter]
            with zipfile.ZipFile(zip_filepath, mode='w') as zf:
                for filepath in filepaths:
                    zf.write(filepath, compress_type=zipfile.ZIP_DEFLATED, arcname=filepath.name)
            logger.info("{0} Successfully created ZIP archive".format(process_badge.identifier))
            return FilePackagerResult([zip_filepath])
        except Exception as e:
            logger.error('{1} ZIP archive creation failed: {0}'.format(str(e), process_badge.identifier))
            return FilePackagerError(str(e), self)

    def _get_filepath(self, shared_info_collection: dict) -> Path:
        """Returns the name for the ZIP archive."""
        filename = "{0}".format(shared_info_collection['BT_FILE_BASENAME'])
        filepath = Path(shared_info_collection['BT_EXPORT_FOLDERPATH'], filename).with_suffix(self.suffix)
        return filepath


class TarFilePackager(AbstractFilePackager):
    """A file packager that creates a ZIP archive from several files.

    Args:
        name (Optional[str]): The name of this file packager. Defaults to "ZIP Archiver".
        overwrite_file (bool): May an existing file  be overwritten? Defaults to False.
    """
    def __init__(self, name: Optional[str] = "TAR Archiver", suffix_filter: Optional[List[str]] = None, overwrite_file: bool = False):
        super().__init__()
        self._name = name
        self.suffix = '.tar.gz'
        if suffix_filter is None:
            suffix_filter = []
        self.suffix_filter = suffix_filter
        self.overwrite_file = overwrite_file

    @property
    def name(self) -> str:
        """Returns the name of the file packager."""
        return self._name

    def package_files(self, filepaths: List[Path], shared_info_collection: dict, process_badge: BoteloProcessBadge) -> Union[FilePackagerResult, FilePackagerError]:
        """Creates a TAR archive from the given filepaths."""
        try:
            logger.info("{0} Creating TAR archive...".format(process_badge.identifier))
            tar_filepath = self._get_filepath(shared_info_collection)
            if not self.overwrite_file and tar_filepath.exists():
                raise FileExistsError("The filepath already exists: {0}".format(tar_filepath))
            if self.suffix_filter:
                filepaths = [filepath for filepath in filepaths if filepath.suffix in self.suffix_filter]
            with tarfile.open(tar_filepath, 'w') as archive:
                for filepath in filepaths:
                    archive.add(filepath, arcname=filepath.name)
            logger.info("{0} Successfully created TAR archive".format(process_badge.identifier))
            return FilePackagerResult([tar_filepath])
        except Exception as e:
            logger.error('{1} TAR archive creation failed: {0}'.format(str(e), process_badge.identifier))
            return FilePackagerError(str(e), self)

    def _get_filepath(self, shared_info_collection: dict) -> Path:
        """Returns the name for the ZIP archive."""
        filename = "{0}".format(shared_info_collection['BT_FILE_BASENAME'])
        filepath = Path(shared_info_collection['BT_EXPORT_FOLDERPATH'], filename).with_suffix(self.suffix)
        return filepath
