from typing import Union, Optional
from abc import ABC, abstractmethod
from pandas import DataFrame
import logging
logger = logging.getLogger(__name__)


class DataFrameManipulatorError(Exception):
    """Raised when a dataframe manipulator fails.

    Args:
        message (str): Human readable string describing the exception.
        df_manipulator (:obj:`AbstractDataFrameManipulator`): An instance of AbstractDataFrameManipulator who raised the exception.

    Attributes:
        message (str): Human readable string describing the exception.
        df_manipulator (:obj:`AbstractDataFrameManipulator`): An instance of AbstractDataFrameManipulator who raised the exception.
    """
    def __init__(self, message, df_manipulator):
        self.message = message
        self.df_manipulator = df_manipulator


class AbstractDataFrameManipulator(ABC):
    """This abstract class describes the interface that must be implemented by a dataframe manipulator.

    Every dataframe manipulator must implement a method named manipulate which returns the manipulated dataframe
    or a DataFrameManipulatorError in case of an exception.

    In addition every dataframe manipulator must have a property named name which returns its name.
    """

    @abstractmethod
    def manipulate(self, df: type(DataFrame), shared_info_collection: dict) -> Union[DataFrameManipulatorError, DataFrame]:
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError


class DataFrameTimezoneStripper(AbstractDataFrameManipulator):
    """An dataframe manipulator used for stripping timezone information from date columns.

    Args:
        name (Optional[str]): The name of this dataframe manipulator. Defaults to "DataFrame Timezone Stripper".
    """

    def __init__(self, name: Optional[str] = "DataFrame Timezone Stripper"):
        self._name = name

    @property
    def name(self):
        """Returns the name of the dataframe manipulator."""
        return self._name

    def manipulate(self, df: DataFrame, shared_info_collection: dict) -> Union[DataFrame, DataFrameManipulatorError]:
        """Tries to strip timezone information from date columns to enable Excel data exports."""
        try:
            logger.info("Stripping timezone information from datetime columns...")
            columns = df.columns
            for column in columns:
                if hasattr(df[column], 'dtype'):
                    if str(df[column].dtype).startswith('datetime64'):
                        logger.info("Identified dataframe column with dates: {0}".format(column))
                        try:
                            df[column] = df[column].dt.tz_convert(None)
                            logger.info("Applied tz_convert(None)")
                        except Exception as e:
                            df[column] = df[column].dt.tz_localize(None)
                            logger.info("Applied tz_localize(None)")
            logger.info("Successfully stripped timezone information")
            return df
        except Exception as e:
            logger.error("Stripping timezone information failed: {0}".format(str(e)))
            return DataFrameManipulatorError(str(e), self)
