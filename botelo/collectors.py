from abc import ABC, abstractmethod
from typing import Union, Optional, List
from pandas import DataFrame
from pathlib import Path
import psycopg
import pandas as pd
from pydrill.client import PyDrill
import random
import logging
logger = logging.getLogger(__name__)


class DataCollectorError(Exception):
    """Raised when a data collector fails.

    Args:
        message (str): Human readable string describing the exception.
        data_collector (:obj:`AbstractDataCollector`): An instance of AbstractDataCollector who raised the exception.

    Attributes:
        message (str): Human readable string describing the exception.
        data_collector (:obj:`AbstractDataCollector`): An instance of AbstractDataCollector who raised the exception.
    """
    def __init__(self, message, data_collector):
        self.message = message
        self.data_collector = data_collector


class AbstractDataCollector(ABC):
    """This abstract class describes the interface that must be implemented by a data collector.

    Every data collector must implement a method named get_dataframe which returns a Pandas DataFrame
    or a DataCollectorError in case of an exception.

    In addition every data collector must have a property named name which returns its name.
    """

    @abstractmethod
    def get_dataframe(self, shared_info_collection: dict) -> Union[DataFrame, DataCollectorError]:
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError


class PassThroughDataFrameCollector(AbstractDataCollector):
    """A data collector which can be used to process an already obtained dataframe.

    Useful in cases where you do not need the dataframe to be retrieved, but just want
    to export a dataframe you already created. Can also be quite handy for test purposes.

    Args:
        dataframe (:obj:`DataFrame`): The dataframe you want to pass through.
        name (Optional[str]): The name of this data collector. Defaults to "Pass-Through DataFrame Collector".

    Raises:
        TypeError: If dataframe is not of type DataFrame.
    """

    def __init__(self, dataframe: DataFrame, name: Optional[str] = "Pass-Through DataFrame Collector"):
        super().__init__()
        self._name = name
        if not isinstance(dataframe, DataFrame):
            raise TypeError("Given dataframe is not of type DataFrame: {0}".format(type(dataframe)))
        self.dataframe = dataframe

    @property
    def name(self) -> str:
        """Returns the name of the data collector."""
        return self._name

    def get_dataframe(self, shared_info_collection: dict) -> DataFrame:
        """Returns the dataframe."""
        return self.dataframe


class TestDataCollector(AbstractDataCollector):
    """A data collector used for testing purposes.

    Args:
        name (Optional[str]): The name of this data collector. Defaults to "Test Data Collector".
        number_of_rows (Optional[int]): The number of rows to be created in the dataframe. Defaults to 15000.
        number_of_columns (Optional[int]): The number of columns to be created in the dataframe. Defaults to 10.

    Raises:
        ValueError: If number_of_rows is equal to or less than 0. If number_of_columns is equal to or less than 0.
    """
    def __init__(self, name: Optional[str] = "Test Data Collector", number_of_rows: int = 15000, number_of_columns: int = 10):
        super().__init__()
        self._name = name
        if number_of_rows <= 0:
            raise ValueError("Number of rows to be generated {0} must nor be equal or lower than 0".format(number_of_rows))
        self._number_of_rows = number_of_rows
        if number_of_columns <= 0:
            raise ValueError("Number of columns to be generated {0} must nor be equal or lower than 0".format(number_of_columns))
        self._number_of_columns = number_of_columns

    @property
    def name(self) -> str:
        """Returns the name of the data collector."""
        return self._name

    def get_dataframe(self, shared_info_collection: dict) -> Union[DataFrame, DataCollectorError]:
        """Creates the dataframe for testing purposes and returns it.

        The values of the records are always random integers.

        Returns:
            A dataframe or a DataCollectorError in case an exception was raised.
        """
        try:
            dataset = []
            for i in range(0, self._number_of_rows):
                data_record = [random.randrange(1, 1250000, 1) for _ in range(0, self._number_of_columns)]
                dataset.append(data_record)
            columns = ['Column_{0}'.format(y+1) for y in range(0, self._number_of_columns)]
            df = pd.DataFrame(data=dataset, columns=columns)
            return df
        except Exception as e:
            return DataCollectorError(str(e), self)


class PostgreSQLDataCollector(AbstractDataCollector):
    """A data collector for PostgreSQL databases.

    Args:
        name (Optional[str]): The name of this data collector. Defaults to "PostgreSQL Data Collector".
        connection_settings (Optional[dict]): The connection settings to connect to the PostgreSQL database.
            These connection settings are directly forwarded to the connect method of psycopg. You can therefore
            use all parameters of the connect method as dictionary keys of the connection settings.
        pre_sql_statements (Optional[List[str]): SQL statements to be executed before the main query. Currently
            psycopg won't allow several SQL commands per query. If you need to execute SQL statements like
            "set work_mem = '2GB';" you can list them in pre_sql_statements.
    """
    def __init__(self, name: Optional[str] = "PostgreSQL Data Collector",
                 connection_settings: Optional[dict] = None,
                 pre_sql_statements: Optional[List[str]] = None,
                 prepared_statement: bool = False):
        super().__init__()
        self._name = name
        self._connection_settings = connection_settings
        self._connection = None
        if pre_sql_statements is None:
            pre_sql_statements = []
        self._pre_sql_statements = pre_sql_statements
        self._prepared_statement = prepared_statement

    @property
    def name(self) -> str:
        """Returns the name of the data collector."""
        return self._name

    @property
    def connection_settings(self) -> dict:
        """Returns the connection settings."""
        return self._connection_settings

    @connection_settings.setter
    def connection_settings(self, connection_settings: dict):
        self._connection_settings = connection_settings

    @property
    def pre_sql_statements(self) -> List[str]:
        """Returns the SQL statements to be executed prior to the main SQL query."""
        return self._pre_sql_statements

    @pre_sql_statements.setter
    def pre_sql_statements(self, pre_sql_statements: List[str]):
        self._pre_sql_statements = pre_sql_statements

    @property
    def prepared_statement(self) -> bool:
        return self._prepared_statement

    @prepared_statement.setter
    def prepared_statement(self, prepared_statement: bool):
        self._prepared_statement = prepared_statement

    def _get_connection(self) -> psycopg.Connection:
        """Returns the connection to the chosen PostgreSQL database."""
        if not self.connection_settings:
            raise ValueError('You must provide connection settings.')

        if self._connection:
            return self._connection

        logger.info("Opening database connection to {0}...".format(self.connection_settings['host']))
        self._connection = psycopg.connect(**self.connection_settings)

        return self._connection

    def get_dataframe(self, shared_info_collection: dict) -> Union[DataFrame, DataCollectorError]:
        """Executes the SQL query against the PostgreSQL database and returns a dataframe."""
        try:
            if 'BT_QUERY' not in shared_info_collection.keys():
                raise KeyError("You must provide the query with the key 'BT_QUERY' in the shared info collection.")

            sql_statement = shared_info_collection['BT_QUERY']

            parameters = None
            if self.prepared_statement:
                if 'BT_QUERY_PARAMETERS' not in shared_info_collection.keys():
                    raise KeyError("When using a prepared statement you must provide the parameters with the key'BT_QUERY_PARAMETERS' in the shared info collection.")
                parameters = shared_info_collection['BT_QUERY_PARAMETERS']

            conn = self._get_connection()
            logger.info("Executing SQL query against database {0}...".format(self.connection_settings['host']))

            cursor = conn.cursor()
            for pre_sql_statement in self.pre_sql_statements:
                cursor.execute(pre_sql_statement)
            if not self.prepared_statement:
                cursor.execute(sql_statement)
            else:
                cursor.execute(sql_statement, parameters)
            conn.commit()

            records = cursor.fetchall()
            column_names = [column[0] for column in cursor.description]
            df = pd.DataFrame(data=records, columns=column_names)

            return df
        except Exception as e:
            return DataCollectorError(str(e), self)
        finally:
            self.close()

    def close(self):
        if self._connection:
            logger.info("Closing database connection to {0}...".format(self.connection_settings['host']))
            self._connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class ApacheDrillDataCollector(AbstractDataCollector):
    """A data collector for Apache Drill.

    Args:
        name (Optional[str]): The name of this data collector. Defaults to "Apache Drill Data Collector".
        connection_settings (Optional[dict]): The connection settings to connect to Apache Drill.
            These connection settings are directly forwarded to PyDrill. You can therefore
            use all parameters of PyDrill() as dictionary keys of the connection settings.
    """
    def __init__(self, name: Optional[str] = "Apache Drill Data Collector", connection_settings: Optional[dict] = None):
        self._name = name
        self._connection_settings = connection_settings
        self.connection = None

    @property
    def name(self) -> str:
        """Returns the name of the data collector."""
        return self._name

    @property
    def connection_settings(self) -> dict:
        """Returns the connection to the chosen Apache Drill instance."""
        return self._connection_settings

    @connection_settings.setter
    def connection_settings(self, connection_settings: dict):
        self._connection_settings = connection_settings

    def _get_connection(self):
        """Returns the connection to the Apache Drill instance."""
        if self.connection:
            return self.connection

        logger.info("Opening Apache Drill connection to {0}...".format(self.connection_settings['host']))
        self.connection = PyDrill(**self.connection_settings)

        return self.connection

    def get_dataframe(self, shared_info_collection: dict) -> Union[DataFrame, DataCollectorError]:
        """Executes the SQL query against Apache Drill and returns a dataframe."""
        try:
            if 'BT_QUERY' not in shared_info_collection.keys():
                raise KeyError("You must provide the query with the key 'BT_QUERY' in the shared´info collection.")

            sql_statement = shared_info_collection['BT_QUERY']

            conn = self._get_connection()
            logger.info("Executing SQL query against Apache Drill {0}...".format(self.connection_settings['host']))

            result = conn.query(sql_statement, self.connection_settings['timeout'])
            df = pd.DataFrame(data=result.rows, columns=result.columns)

            return df
        except Exception as e:
            return DataCollectorError(str(e), self)

    def __enter__(self):
        return self


class CSVFileDataCollector(AbstractDataCollector):
    """A data collector that reads a CSV file in chunks and returns a dataframe.

    Args:
        filepath (:obj:`Path`): The filepath of the CSV file
        chunksize (int): Size of a single chunk to read from the CSV file. Defaults to 10000.
        csv_kwargs (Optional[dict]): A dictionary with settings for reading the CSV file. These settings are
            directly forwarded to the read_csv method of pandas. You can therefore use all
            parameters of it as dictionary keys.
        name (Optional[str]): The name of this data collector. Defaults to "CSV File Data Collector".
    """
    def __init__(self, filepath: Path, chunk_size: int = 10000,  csv_kwargs: Optional[dict] = None, name: Optional[str] = "CSV File Data Collector"):
        self._name = name
        self.filepath = filepath
        self.chunk_size = chunk_size
        if csv_kwargs is None:
            csv_kwargs = {}
        self._csv_kwargs = csv_kwargs

    @property
    def name(self) -> str:
        """Returns the name of the data collector."""
        return self._name

    def get_dataframe(self, shared_info_collection: dict) -> Union[DataFrame, DataCollectorError]:
        """Reads a CSV file in chunks and returns a dataframe."""
        try:
            if not self.filepath.exists():
                raise FileNotFoundError("The CSV file does not exist: {0}".format(self.filepath))

            logger.info("Trying to read the CSV file...")
            tp = pd.read_csv(self.filepath, iterator=True, chunksize=self.chunk_size, **self._csv_kwargs)
            df = pd.concat(tp, ignore_index=True)
            logger.info("Successfully read the CSV file")
            return df
        except Exception as e:
            logger.error("Reading the CSV file failes: {0}".format(str(e)))
            return DataCollectorError(str(e), self)
