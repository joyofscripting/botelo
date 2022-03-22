from typing import Union, Optional
from pathlib import Path
from abc import ABC, abstractmethod
from botelo.structures import BoteloProcessBadge
from pandas import DataFrame
from pandas import ExcelWriter
import logging
logger = logging.getLogger(__name__)


class DataExportError(Exception):
    """Raised when a data export fails.

    Args:
        message (str): Human readable string describing the exception.
        data_exporter (:obj:`AbstractDataExporter`): An instance of AbstractDataExporter who raised the exception.

    Attributes:
        message (str): Human readable string describing the exception.
        data_exporter (:obj:`AbstractDataExporter`): An instance of AbstractDataExporter who raised the exception."""
    def __init__(self, message, data_exporter):
        self.message = message
        self.data_exporter = data_exporter


class DataExportResult(object):
    """Contains the result of the data export.

    Args:
        filepath (:obj:`Path`): The path of the created export file.

    Attributes:
        filepath (:obj:`Path`): The path of the created export file.
    """
    def __init__(self, filepath: Path):
        self.filepath = filepath


class AbstractDataExporter(ABC):
    """This abstract class describes the interface that must be implemented by a data exporter.

    Every data exporter must implement a method named export which returns a DataExportResult
    or a DataExportError in case of an exception.

    In addition every data exporter must have a property named name which returns its name.
    """

    @abstractmethod
    def export(self, df: DataFrame, shared_info_collection: dict, process_badge: BoteloProcessBadge) -> Union[DataExportResult, DataExportError]:
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError


class ExcelDataExporter(AbstractDataExporter):
    """A data exporter to export a dataframe to an Excel file.

    Args:
        name (Optional[str]): The name of this data exporter. Defaults to "Excel Data Exporter".
        ew_kwargs (Optional[dict]): A dictionary with settings for the ExcelWriter. These settings are
            directly used while creating the ExcelWriter instance. You can therefore use all
            parameters of ExcelWriter as dictionary keys.
        df_kwargs (Optional[dict]): A dictionary with settings for the DataFrame. These settings are
            directly used while exporting the DataFrame with the to_excel() method. You can therefore use all
            named keywords of this method as dictionary keys.
        overwrite_file (bool): May an existing file  be overwritten? Defaults to False.
    """
    def __init__(self, name: Optional[str] = "Excel Data Exporter", ew_kwargs: Optional[dict] = None, df_kwargs: Optional[dict] = None, overwrite_file: bool = False):
        super().__init__()
        self._name = name
        self.suffix = '.xlsx'
        if not ew_kwargs:
            ew_kwargs = {'engine': 'xlsxwriter'}
        if not df_kwargs:
            df_kwargs = {}
        self._ew_kwargs = ew_kwargs
        self._df_kwargs = df_kwargs
        self.overwrite_file = overwrite_file

    @property
    def name(self) -> str:
        """Returns the name of the data exporter."""
        return self._name

    def export(self, df: DataFrame, shared_info_collection: dict, process_badge: BoteloProcessBadge) -> Union[DataExportResult, DataExportError]:
        """Exports a dataframe to an Excel file.

        Returns:
            DataExportResult containing the filepath of the exported file or DataExportError in case an exception was raised.
        """
        try:
            number_of_rows = len(df)
            if number_of_rows > 1048576:
                raise ValueError("The chunk size must not exceed 1048576, the maximum number of rows in an XLSX file.")

            filepath = self._get_filepath(shared_info_collection, process_badge)
            if not self.overwrite_file and filepath.exists():
                raise FileExistsError("File already exists: {0}".format(filepath))
            logger.info("{2} Exporting {0} rows to Excel file {1}...".format(len(df), filepath, process_badge.identifier))
            writer = ExcelWriter(filepath, **self._ew_kwargs)
            df.to_excel(writer, **self._df_kwargs)
            writer.save()
            logger.info("{0} Successfully saved the Excel file".format(process_badge.identifier))
            return DataExportResult(filepath)
        except Exception as e:
            logger.error("{1} Export failed: {0}".format(str(e), process_badge.identifier))
            return DataExportError(str(e), self)

    def _get_filepath(self, shared_info_collection: dict, process_badge: BoteloProcessBadge) -> Path:
        """Returns the filepath for the Excel file to be created."""
        if process_badge.number_of_processes > 1:
            filename = "{0}_{1}".format(shared_info_collection['BT_FILE_BASENAME'], str(process_badge.process_number))
        else:
            filename = "{0}".format(shared_info_collection['BT_FILE_BASENAME'])
        filepath = Path(shared_info_collection['BT_EXPORT_FOLDERPATH'], filename).with_suffix(self.suffix)
        return filepath


class CSVDataExporter(AbstractDataExporter):
    """A data exporter to export a dataframe to a CSV file.

    Args:
        name (Optional[str]): The name of this data exporter. Defaults to "CSV Data Exporter".
        df_kwargs (Optional[dict]): A dictionary with settings for the DataFrame. These settings are
            directly used while exporting the DataFrame with the to_csv() method. You can therefore use all
            named keywords of this method as dictionary keys.
        overwrite_file (bool): May an existing file  be overwritten? Defaults to False.
    """
    def __init__(self, name: Optional[str] = "CSV Data Exporter", df_kwargs: Optional[dict] = None, overwrite_file: bool = False):
        super().__init__()
        self._name = name
        self.suffix = '.csv'
        if not df_kwargs:
            df_kwargs = {}
        self._df_kwargs = df_kwargs
        self.overwrite_file = overwrite_file

    @property
    def name(self) -> str:
        """Returns the name of the data exporter."""
        return self._name

    def export(self, df: DataFrame, shared_info_collection: dict, process_badge: BoteloProcessBadge) -> Union[DataExportResult, DataExportError]:
        """Exports a dataframe to a CSV file.

        Returns:
            DataExportResult containing the filepath of the exported file or DataExportError in case an exception was raised.
        """
        try:
            filepath = self._get_filepath(shared_info_collection, process_badge)
            if not self.overwrite_file and filepath.exists():
                raise FileExistsError("File already exists: {0}".format(filepath))
            logger.info("{2} Exporting {0} rows to CSV file {1}...".format(len(df), filepath, process_badge.identifier))
            df.to_csv(filepath, **self._df_kwargs)
            logger.info("{0} Successfully saved the CSV file".format(process_badge.identifier))
            return DataExportResult(filepath)
        except Exception as e:
            logger.error("{1} Export failed: {0}".format(str(e), process_badge.identifier))
            return DataExportError(str(e), self)

    def _get_filepath(self, shared_info_collection: dict, process_badge: BoteloProcessBadge) -> Path:
        """Returns the filepath for the CSV file to be created."""
        if process_badge.number_of_processes > 1:
            filename = "{0}_{1}".format(shared_info_collection['BT_FILE_BASENAME'], str(process_badge.process_number))
        else:
            filename = "{0}".format(shared_info_collection['BT_FILE_BASENAME'])
        filepath = Path(shared_info_collection['BT_EXPORT_FOLDERPATH'], filename).with_suffix(self.suffix)
        return filepath


class ParquetDataExporter(AbstractDataExporter):
    """A data exporter to export a dataframe to a Parquet file.

    Args:
        name (Optional[str]): The name of this data exporter. Defaults to "Parquet Data Exporter".
        df_kwargs (Optional[dict]): A dictionary with settings for the DataFrame. These settings are
            directly used while exporting the DataFrame with the to_parquet() method. You can therefore use all
            named keywords of this method as dictionary keys.
        overwrite_file (bool): May an existing file  be overwritten? Defaults to False.
    """
    def __init__(self, name: Optional[str] = "Parquet Data Exporter", df_kwargs: Optional[dict] = None, overwrite_file: bool = False):
        super().__init__()
        self._name = name
        self.suffix = '.parquet'
        if not df_kwargs:
            df_kwargs = {}
        self._df_kwargs = df_kwargs
        self.overwrite_file = overwrite_file

    @property
    def name(self) -> str:
        """Returns the name of the data exporter."""
        return self._name

    def export(self, df: DataFrame, shared_info_collection: dict, process_badge: BoteloProcessBadge) -> Union[DataExportResult, DataExportError]:
        """Exports a dataframe to a Parquet file.

        Returns:
            DataExportResult containing the filepath of the exported file or DataExportError in case an exception was raised.
        """
        try:
            filepath = self._get_filepath(shared_info_collection, process_badge)
            if not self.overwrite_file and filepath.exists():
                raise FileExistsError("File already exists: {0}".format(filepath))
            logger.info("{2} Exporting {0} rows to Parquet file {1}...".format(len(df), filepath, process_badge.identifier))
            df.to_parquet(filepath, **self._df_kwargs)
            logger.info("{0} Successfully saved the Parquet file".format(process_badge.identifier))
            return DataExportResult(filepath)
        except Exception as e:
            logger.error("{1} Export failed: {0}".format(str(e), process_badge.identifier))
            return DataExportError(str(e), self)

    def _get_filepath(self, shared_info_collection: dict, process_badge: BoteloProcessBadge) -> Path:
        """Returns the filepath for the Parquet file to be created."""
        if process_badge.number_of_processes > 1:
            filename = "{0}_{1}".format(shared_info_collection['BT_FILE_BASENAME'], str(process_badge.process_number))
        else:
            filename = "{0}".format(shared_info_collection['BT_FILE_BASENAME'])
        filepath = Path(shared_info_collection['BT_EXPORT_FOLDERPATH'], filename).with_suffix(self.suffix)
        return filepath
