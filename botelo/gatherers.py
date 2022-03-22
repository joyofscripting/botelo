from abc import ABC, abstractmethod
from typing import Union, Optional
from datetime import datetime
import requests
import logging
logger = logging.getLogger(__name__)


class InfoGathererError(Exception):
    """Raised when an info gatherer fails.

    Args:
        message (str): Human readable string describing the exception.
        info_gatherer (:obj:`AbstractInfoGatherer`): An instance of AbstractInfoGatherer who raised the exception.

    Attributes:
        message (str): Human readable string describing the exception.
        info_gatherer (:obj:`AbstractInfoGatherer`): An instance of AbstractInfoGatherer who raised the exception.
    """
    def __init__(self, message, info_gatherer):
        self.message = message
        self.info_gatherer = info_gatherer


class AbstractInfoGatherer(ABC):
    """This abstract class describes the interface that must be implemented by an info gatherer.

    Every info gatherer must implement a method named get_info which returns a dictionary
    or a InfoGathererError in case of an exception.

    In addition every info gatherer must have a property named name which returns its name.
    """

    @abstractmethod
    def get_info(self, shared_info_collection: dict) -> Union[dict, InfoGathererError]:
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError


class TestInfoGatherer(AbstractInfoGatherer):
    """An info gatherer used for testing purposes.

    Args:
        name (Optional[str]): The name of this info gatherer. Defaults to "Test Info Gatherer".
        key (Optional[object]): The key you want to use for the information. Defaults to 'test_key'.
        value (Optional[object]): The value you want to use for the information. Defaults to 'test_value'.
    """
    def __init__(self, name: Optional[str] = "Test Info Gatherer", key: Optional[object] = "test_key", value: Optional[object] = 'test_value'):
        self._name = name
        self._key = key
        self._value = value

    @property
    def name(self) -> str:
        """Returns the name of the info gatherer."""
        return self._name

    def get_info(self, shared_info_collection: dict) -> dict:
        """Returns a dictionary containing the test key and value."""
        return {self._key: self._value}


class JiraIssueTitleGatherer(AbstractInfoGatherer):
    """An info gatherer used for getting the title of a Jira issue.

    Args:
        info_key (str): The key under which to save the gathered Jira issue title in the shared info collection.
        name (Optional[str]): The name of this info gatherer. Defaults to "Jira Issue Title Gatherer".
    """
    def __init__(self, info_key: str, name: Optional[str] = 'Jira Issue Title Gatherer'):
        self._name = name
        self.info_key = info_key

    @property
    def name(self) -> str:
        """Returns the name of the info gatherer."""
        return self._name

    def get_info(self, shared_info_collection: dict) -> Union[dict, InfoGathererError]:
        """Returns a dictionary containing the title of the Jira issue."""
        try:
            for info_key in ['jira_issue', 'jira_base_url', 'jira_user', 'jira_password']:
                if info_key not in shared_info_collection.keys():
                    raise KeyError("Key {0} not found in shared info collection.".format(info_key))

            logger.info("Trying to fetch title from Jira issue {0}...".format(shared_info_collection['jira_issue']))
            url = '{0}/rest/api/2/issue/{1}'.format(shared_info_collection['jira_base_url'], shared_info_collection['jira_issue'])
            r = requests.get(url, auth=(shared_info_collection['jira_user'], shared_info_collection['jira_password']))
            r.raise_for_status()
            issue_title = r.json()['fields']['summary']
            logger.info("Successfully fetched title: {0}".format(issue_title))
            return {self.info_key: issue_title}
        except Exception as e:
            logger.error("Fetching Jira issue title failed: {0}".format(str(e)))
            return InfoGathererError(str(e), self)


class FormattedDatetimeGatherer(AbstractInfoGatherer):
    """An info gatherer used for getting a formatted datetime.

    Args:
        key (str): The key under which to save the formatted date in the shared info collection.
        strformat (str): The string format of your choice, e.g. '%m/%d/%Y, %H:%M:%S'
        date_time (:obj:`datetime`): The date you want to use. If no datetime is given
            it will be set to the moment when the get_info method is called.
        name (Optional[str]): The name of this info gatherer. Defaults to "Formatted Datetime Gatherer".
    """
    def __init__(self, key, strformat: str, date_time: Optional[datetime] = None, name: Optional[str] = 'Formatted Datetime Gatherer'):
        self._name = name
        self.strformat = strformat
        self.date_time = date_time
        self.key = key

    @property
    def name(self) -> str:
        """Returns the name of the info gatherer."""
        return self._name

    def get_info(self, shared_info_collection: dict) -> Union[dict, InfoGathererError]:
        """Returns a dictionary containing a formatted datetime."""
        try:
            logger.info("Trying to gather formatted datetime...")
            if self.date_time is None:
                self.date_time = datetime.now()
            formatted_date_time = self.date_time.strftime(self.strformat)
            logger.info("Successfully gathered formatted datetime: {0}".format(formatted_date_time))
            return {self.key: formatted_date_time}
        except Exception as e:
            logger.error("Gathering formatted datetime failed: {0}".format(str(e)))
            return InfoGathererError(str(e), self)


class BaseFilenameGatherer(AbstractInfoGatherer):
    """An info gatherer used to get the file basename used for naming export files.

    Args:
        key (str): The key under which to save the file basename in the shared info collection.
        formatted_str (str): A format string containing keys from the shared info collection,
            e.g. 'Sales_report_{formatted_datetime}_{connection_name}_{jira_issue}'
        name (Optional[str]): The name of this info gatherer. Defaults to "Base Filename Gatherer".
    """
    def __init__(self, key: str, formatted_str: str, name: Optional[str] = "Base Filename Gatherer"):
        self.key = key
        self.formatted_str = formatted_str
        self._name = name

    @property
    def name(self) -> str:
        """Returns the name of the info gatherer."""
        return self._name

    def get_info(self, shared_info_collection: dict) -> Union[dict, InfoGathererError]:
        """Returns a dictionary containing a file basename."""
        try:
            logger.info("Trying to gather formatted base filename...")
            base_filename = self.formatted_str.format(**shared_info_collection)
            logger.info("Successfully gathered formatted base filename: {0}".format(base_filename))
            return {self.key: base_filename}
        except Exception as e:
            logger.error("Gathering formatted base filename failed: {0}".format(str(e)))
            return InfoGathererError(str(e), self)
