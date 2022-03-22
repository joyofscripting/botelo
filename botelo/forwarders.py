from abc import ABC, abstractmethod
from typing import Union, Optional, List
from pathlib import Path
import requests
import json
from botelo.structures import BoteloFilesBucket
import logging

logger = logging.getLogger(__name__)


class ForwarderError(Exception):
    """Raised when a forwarder fails.

    Args:
        message (str): Human readable string describing the exception.
        forwarder (:obj:`AbstractForwarder`): An instance of AbstractForwarder who raised the exception.

    Attributes:
        message (str): Human readable string describing the exception.
        forwarder (:obj:`AbstractForwarder`): An instance of AbstractForwarder who raised the exception."""

    def __init__(self, message, forwarder):
        self.message = message
        self.forwarder = forwarder


class ForwarderResult(object):
    """Contains the result of the forwarder.

    Args:
        result (obj): The result of the forwarder.

    Attributes:
        result (obj): The result of the forwarder.
    """

    def __init__(self, result):
        self.result = result


class AbstractForwarder(ABC):
    """This abstract class describes the interface that must be implemented by a forwarder.

    Every forwarder must implement a method named forward_files which returns a ForwarderResult
    or a ForwarderError in case of an exception.

    In addition every forwarder must have a property named name which returns its name.
    """

    @abstractmethod
    def forward_files(self, files_bucket: BoteloFilesBucket, shared_info_collection: dict) -> Union[ForwarderResult, ForwarderError]:
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError


class TestForwarder(AbstractForwarder):
    """A forwarder for testing purposes.

    Args:
        name (Optional[str]): The name of this forwarder. Defaults to "Test Forwarder".
    """

    def __init__(self, name: Optional[str] = "Test Forwarder"):
        self._name = name

    @property
    def name(self) -> str:
        """Returns the name of the forwarder."""
        return self._name

    def forward_files(self, files_bucket: BoteloFilesBucket, shared_info_collection: dict) -> Union[ForwarderResult, ForwarderError]:
        """Logs an info message and returns an instance of ForwarderResult for testing purposes."""
        logger.info("This is just a test forwarder: {0}".format(self._name))
        return ForwarderResult('Success')


class JiraForwarder(AbstractForwarder):
    """Uploads files to a Jira issue and optionally creates a corresponding comment listing the uploaded files.

    Args:
        jira_comment (Optional[str]): An optional comment which can be written to the Jira issue naming the uploaded files.
            You can use a format string with the following keywords:
            {count_files}: The number of uploaded files.
            {filenames}: The names of the files as a clickable link in Jira to download the files.
        suffix_filter (Optional[List[str]]): Optional list of suffixes. Enables uploading only files with a certain suffix.
        name (Optional[str]): The name of this forwarder. Defaults to "Jira Forwarder".
    """

    def __init__(self, jira_comment: Optional[str] = None, suffix_filter: Optional[List[str]] = None, name: Optional[str] = "Jira Forwarder"):
        if suffix_filter is None:
            suffix_filter = []
        self._suffix_filter = suffix_filter
        self._name = name
        self._jira_comment = jira_comment

    @property
    def name(self) -> str:
        """Returns the name of the forwarder."""
        return self._name

    def forward_files(self, files_bucket: BoteloFilesBucket, shared_info_collection: dict) -> Union[ForwarderResult, ForwarderError]:
        """Uploads files as attachments to a Jira issue and optionally adds a corresponding comment."""
        uploaded_filepaths = []

        try:
            for info_key in ['jira_issue', 'jira_base_url', 'jira_user', 'jira_password']:
                if info_key not in shared_info_collection.keys():
                    raise KeyError("Key {0} not found in shared info collection.".format(info_key))

            url = '{0}/rest/api/2/issue/{1}/attachments'.format(shared_info_collection['jira_base_url'], shared_info_collection['jira_issue'])
            headers = {"X-Atlassian-Token": "nocheck"}

            filepaths = files_bucket.get_all_filepaths()
            if self._suffix_filter:
                filepaths = [filepath for filepath in filepaths if filepath.suffix in self._suffix_filter]
            for filepath in filepaths:
                if filepath.exists() and filepath not in uploaded_filepaths:
                    logger.info("Uploading file {0} to {1}...".format(filepath, url))
                    with open(filepath, 'rb') as openfile:
                        files = {'file': openfile}
                        r = requests.post(url, auth=(shared_info_collection['jira_user'], shared_info_collection['jira_password']), files=files, headers=headers)
                    logger.debug("HTTP status code: {0}".format(r.status_code))
                    r.raise_for_status()
                    logger.info("Upload completed")
                    uploaded_filepaths.append(filepath)
        except Exception as e:
            logger.error("Upload failed: {0}".format(str(e)))
            return ForwarderError(str(e), self)

        if self._jira_comment:
            self.write_comment(uploaded_filepaths, shared_info_collection)

        return ForwarderResult("JIRA Forwarder successfully uploaded files")

    def write_comment(self, filepaths: List[Path], shared_info_collection: dict):
        """Writes a comment to a Jira issue describing the uploaded files."""
        filenames = '\n'.join([' [^' + filepath.name + ']' for filepath in filepaths])
        count_files = len(filepaths)
        jira_comment = self._jira_comment.format(**{'count_files': count_files, 'filenames': filenames})

        url = '{0}/rest/api/2/issue/{1}/comment'.format(shared_info_collection['jira_base_url'], shared_info_collection['jira_issue'])
        headers = {'Content-Type': 'application/json'}
        data = json.dumps({'body': jira_comment})

        logger.info("Adding comment to {0}...".format(url))
        try:
            r = requests.post(url, auth=(shared_info_collection['jira_user'], shared_info_collection['jira_password']), data=data, headers=headers)
            logger.debug("HTTP status code: {0}".format(r.status_code))
            r.raise_for_status()
            logger.info("Comment successfully added")
        except Exception as e:
            logger.warning("Adding the comment failed: {0}".format(str(e)))
