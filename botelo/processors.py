import os
import copy
from typing import Optional
from typing import List
from datetime import datetime
from collections import namedtuple
from multiprocessing import Pool
from pandas import DataFrame
import botelo.config as config
from botelo.jobs import ExportJob
from botelo.structures import BoteloProcessBadge
from botelo.gatherers import AbstractInfoGatherer
from botelo.gatherers import InfoGathererError
from botelo.manipulators import AbstractDataFrameManipulator
from botelo.manipulators import DataFrameManipulatorError
from botelo.collectors import AbstractDataCollector
from botelo.collectors import DataCollectorError
from botelo.exporters import AbstractDataExporter
from botelo.exporters import DataExportResult
from botelo.exporters import DataExportError
from botelo.filesavers import AbstractTextSaver
from botelo.finishers import AbstractFileFinisher
from botelo.finishers import FileFinisherResult
from botelo.finishers import FileFinisherError
from botelo.filesavers import FileSaverResult
from botelo.filesavers import FileSaverError
from botelo.packagers import AbstractFilePackager
from botelo.packagers import FilePackagerResult
from botelo.packagers import FilePackagerError
from botelo.forwarders import AbstractForwarder
from botelo.forwarders import ForwarderError
from botelo.cleaners import AbstractCleaner
from botelo.cleaners import CleanerError
from botelo.structures import BoteloFilesBucket
import logging
logger = logging.getLogger(__name__)


class BoteloError(Exception):
    """Raised when Botelo fails.

    Args:
        message (str): Human readable string describing the exception.

    Attributes:
        message (str): Human readable string describing the exception.
    """
    def __init__(self, message):
        self.message = message


class BoteloDuration(object):

    def __init__(self, start_time=None, end_time=None):
        self._start_time = start_time
        self._end_time = end_time
        self._duration = None

    @property
    def start_time(self):
        return self._start_time

    @start_time.setter
    def start_time(self, start_time):
        self._start_time = start_time

    @property
    def end_time(self):
        return self._end_time

    @end_time.setter
    def end_time(self, end_time):
        self._end_time = end_time

    @property
    def duration(self):
        if self._start_time is None or self._end_time is None:
            self._duration = None
        else:
            self._duration = self._end_time - self._start_time
        return self._duration


class BoteloJobReport(object):
    """Helps to track and save the duration of each Botelo job."""

    STEP_JOB = 'job'
    STEP_DATA_COLLECTION = 'data_collection'
    STEP_INFO_GATHERING = 'info_gathering'
    STEP_DF_MANIPULATION = 'df_manipulation'
    STEP_DATA_EXPORT = 'data_export'
    STEP_QUERY_SAVING = 'query_saving'
    STEP_FINISHING_FILES = 'finishing_files'
    STEP_FILE_PACKAGING = 'file_packagers'
    STEP_FILE_FORWARDING = 'forwarders'
    STEP_CLEANING_UP = 'cleaners'

    def __init__(self):
        self._steps = (BoteloJobReport.STEP_JOB,
                       BoteloJobReport.STEP_DATA_COLLECTION,
                       BoteloJobReport.STEP_INFO_GATHERING,
                       BoteloJobReport.STEP_DF_MANIPULATION,
                       BoteloJobReport.STEP_DATA_EXPORT,
                       BoteloJobReport.STEP_QUERY_SAVING,
                       BoteloJobReport.STEP_FINISHING_FILES,
                       BoteloJobReport.STEP_FILE_PACKAGING,
                       BoteloJobReport.STEP_FILE_FORWARDING,
                       BoteloJobReport.STEP_CLEANING_UP,)
        self._durations = {}
        for step in self._steps:
            self._durations[step] = BoteloDuration()
        self._count_records = None

    @property
    def count_records(self):
        return self._count_records

    @count_records.setter
    def count_records(self, count_records):
        self._count_records = count_records

    def duration(self, step: str):
        """Returns the duration of the given Botelo process step.

        Args:
            step (str): Botelo process step, e.g. 'data_collection'.

        Returns:
            The duration of the given Botelo process step.

        Raises:
            ValueError: When the Botelo process step is unknown.
        """
        if step not in self._steps:
            raise ValueError("Unknown step: {0}".format(step))
        return self._durations[step].duration

    def kickoff(self, step: str):
        """Sets the start time for a Botelo processing step.

        Args:
            step (str): Botelo process step, e.g. 'data_collection'.

        Raises:
            ValueError: When the Botelo process step is unknown.
        """
        if step not in self._steps:
            raise ValueError("Unknown step: {0}".format(step))
        start_time = datetime.now().replace(microsecond=0)
        self._durations[step].start_time = start_time

    def checkered_flag(self, step: str):
        """Sets the end time for a Botelo processing step.

        Args:
            step (str): Botelo process step, e.g. 'data_collection'.

        Raises:
            ValueError: When the Botelo process step is unknown.
        """
        if step not in self._steps:
            raise ValueError("Unknown step: {0}".format(step))
        end_time = datetime.now().replace(microsecond=0)
        self._durations[step].end_time = end_time

    def log_durations(self):
        """Logs the duration of each Botelo process step."""
        label_items = [
            ('Data collection', BoteloJobReport.STEP_DATA_COLLECTION,),
            ('Info gathering', BoteloJobReport.STEP_INFO_GATHERING,),
            ('Dataframe manipulation', BoteloJobReport.STEP_DF_MANIPULATION,),
            ('Data export', BoteloJobReport.STEP_DATA_EXPORT,),
            ('Query saving', BoteloJobReport.STEP_QUERY_SAVING,),
            ('Finishing files', BoteloJobReport.STEP_FINISHING_FILES,),
            ('File packaging', BoteloJobReport.STEP_FILE_PACKAGING,),
            ('Forwarding files', BoteloJobReport.STEP_FILE_FORWARDING,),
            ('Cleaning up', BoteloJobReport.STEP_CLEANING_UP,),
            ('Job', BoteloJobReport.STEP_JOB, )
        ]
        for label_item in label_items:
            logger.info("{0}: {1}".format(label_item[0], self.duration(label_item[1])))

    def log_count_records(self):
        logger.info("Count of exported records: {0}".format(self.count_records))


class Botelo(object):
    """Botelo processes a list of given export jobs.

    Args:
        export_jobs (:obj:`list` of :obj:`ExportJob`, optional): A list of ExportJob instances to process
        parallel_processes (:obj:`int`, optional): Number of parallel processes used for data exports.
            Default: 1 for single core machines or half of available cores for machines with multiple cores
    """

    def __init__(self, export_jobs: Optional[List[ExportJob]] = None, parallel_processes: int = None):
        if not export_jobs:
            export_jobs = []
        self._export_jobs = export_jobs
        self._shared_info_collection = {}

        if not parallel_processes:
            parallel_processes = self._get_parallel_processes_default()
        self._validate_parallel_processes(parallel_processes)

        self._parallel_processes = parallel_processes
        self._files_bucket = None

    @property
    def parallel_processes(self):
        """The maximum number of parallel processes to be used for data exports and file finishers."""
        return self._parallel_processes

    @property
    def files_bucket(self):
        if self._files_bucket is not None:
            files_bucket = copy.deepcopy(self._files_bucket)
            return files_bucket
        else:
            return None

    @parallel_processes.setter
    def parallel_processes(self, parallel_processes):
        self._validate_parallel_processes(parallel_processes)
        self._parallel_processes = parallel_processes

    @property
    def shared_info_collection(self):
        """Returns a copy of the shared info collection dictionary."""
        return copy.deepcopy(self._shared_info_collection)

    def add_shared_info(self, info_key, info_value):
        """Add a key-value pair of information to the shared info collection.

        Args:
            info_key (obj): The key of the information.
            info_value (obj): The value of the information.

        Raises:
             ValueError: If the key is already present in the shared info collection.
        """
        if info_key not in self._shared_info_collection.keys():
            self._shared_info_collection[info_key] = info_value
        else:
            raise ValueError("Info key {0} is already present in the shared info collection".format(info_key))

    def process_export_jobs(self):
        """Starts the subsequent processing of all specified export jobs."""
        if not self._export_jobs:
            raise BoteloError("No export jobs found")

        total_report = BoteloJobReport()
        total_report.kickoff(BoteloJobReport.STEP_JOB)
        logger.info("Starting to process {0} export job(s)...".format(len(self._export_jobs)))
        export_job_num = 1

        for export_job in self._export_jobs:
            job_report = BoteloJobReport()
            job_report.kickoff(BoteloJobReport.STEP_JOB)

            logger.info("= Export job no. {0}: {1}".format(export_job_num, export_job.name))
            # Resetting the files bucket for the current export job
            self._files_bucket = None
            self._files_bucket = BoteloFilesBucket()
            self._shared_info_collection = copy.deepcopy(export_job.shared_info_collection)

            # Retrieving the data to export by executing the given data collector
            job_report.kickoff(BoteloJobReport.STEP_DATA_COLLECTION)
            df = self._perform_data_collection(export_job)
            job_report.checkered_flag(BoteloJobReport.STEP_DATA_COLLECTION)
            # if no dataframe was returned or the dataframe was empty we continue with the next export job
            if df is None:
                continue

            # Gathering further infos from the specified info gatherers
            # and pushing them to the shared info collection
            job_report.kickoff(BoteloJobReport.STEP_INFO_GATHERING)
            self._perform_info_gathering(export_job)
            job_report.checkered_flag(BoteloJobReport.STEP_INFO_GATHERING)

            # In case the info gatherers did not provide informations for the export folder name
            # and file basename we fall back to defaults

            if 'BT_FILE_BASENAME' not in self._shared_info_collection.keys():
                logger.warning("Key file_basename not found in shared info collection. Adding default value from config: {0}".format(config.DEFAULT_FILE_BASENAME))
                self._shared_info_collection['BT_FILE_BASENAME'] = config.DEFAULT_FILE_BASENAME

            if 'BT_EXPORT_FOLDERPATH' not in self._shared_info_collection.keys():
                logger.warning("Key export_folderpath not found in shared info collection. Adding default value from config: {0}".format(config.DEFAULT_EXPORT_FOLDERPATH))
                self._shared_info_collection['BT_EXPORT_FOLDERPATH'] = config.DEFAULT_EXPORT_FOLDERPATH

            # Manipulating the retrieved dataframe in case manipulators are given
            # This can be useful to perform additional manipulations, e.g. changing data types
            # in the dataframe or cleaning its data. By separating it from the data collection
            # we have a better separation of concerns.
            job_report.kickoff(BoteloJobReport.STEP_DF_MANIPULATION)
            df = self._perform_df_manipulation(df, export_job)
            job_report.checkered_flag(BoteloJobReport.STEP_DF_MANIPULATION)
            job_report.count_records = df.shape[0]

            # Determining the chunks in which to split the retrieved data
            # and exporting these chunks with the given data exporter
            # In order to speed up the data export we try to use parallel data export processes
            job_report.kickoff(BoteloJobReport.STEP_DATA_EXPORT)
            self._perform_data_export(df, export_job)
            job_report.checkered_flag(BoteloJobReport.STEP_DATA_EXPORT)

            # If the user decided to specify a query saver we save the query to a file
            job_report.kickoff(BoteloJobReport.STEP_QUERY_SAVING)
            self._perform_query_saving(export_job)
            job_report.checkered_flag(BoteloJobReport.STEP_QUERY_SAVING)

            # Finishing exported files
            job_report.kickoff(BoteloJobReport.STEP_FINISHING_FILES)
            self._perform_finishing_files(export_job)
            job_report.checkered_flag(BoteloJobReport.STEP_FINISHING_FILES)

            # Packaging files into archives
            job_report.kickoff(BoteloJobReport.STEP_FILE_PACKAGING)
            self._perform_file_packagers(export_job)
            job_report.checkered_flag(BoteloJobReport.STEP_FILE_PACKAGING)

            logger.info("»» Files report:")
            self.files_bucket.log_filepaths()

            # Forwarding files
            job_report.kickoff(BoteloJobReport.STEP_FILE_FORWARDING)
            self._perform_forwarders(export_job)
            job_report.checkered_flag(BoteloJobReport.STEP_FILE_FORWARDING)

            # Cleaning up the mess we created
            job_report.kickoff(BoteloJobReport.STEP_CLEANING_UP)
            self._perform_cleaners(export_job)
            job_report.checkered_flag(BoteloJobReport.STEP_CLEANING_UP)

            # End of job
            job_report.checkered_flag(BoteloJobReport.STEP_JOB)

            logger.info("»» Job report:")
            job_report.log_count_records()
            job_report.log_durations()
            export_job_num += 1

        # End of botelo
        total_report.checkered_flag(BoteloJobReport.STEP_JOB)
        logger.info("Botelo finished {1} job(s) successfully in {0}".format(total_report.duration(BoteloJobReport.STEP_JOB), len(self._export_jobs)))

    def _perform_data_collection(self, export_job: ExportJob) -> Optional[DataFrame]:
        """Wraps the process of collecting data using the data collector specified in the export job.

        Args:
            export_job (:obj:`ExportJob`): The export job which is currently processed.

        Returns:
            A DataFrame when the data collection was successful and returned rows or None if the DataFrame is empty.

        Raises:
            BoteloError: If no data collector was specified or the data collection raised an exception.
        """
        logger.info("»» Data collection")
        if not export_job.data_collector:
            raise BoteloError("No data collector found in export job {0}".format(export_job.name))

        df = self._retrieve_dataframe(export_job.data_collector)

        row_count = len(df)
        if row_count == 0:
            logger.info("{0} returned an empty dataframe".format(export_job.data_collector.name))
            return None
        return df

    def _perform_info_gathering(self, export_job: ExportJob):
        """Wraps the process of gathering infos using the info gatherers specified in the export job.

        Args:
            export_job (:obj:`ExportJob`): The export job which is currently processed.

        Raises:
            BoteloError: If the process of info gathering raised an exception.
        """
        logger.info("»» Gathering further infos")
        if export_job.info_gatherers:
            self._gather_infos(export_job.info_gatherers)
        else:
            logger.info("No info gatherers defined, continuing without gathering further info")

    def _perform_df_manipulation(self, df: DataFrame, export_job: ExportJob) -> DataFrame:
        """Wraps the process of manipulating the retrieved dataframe using the manipulators specified in the export job.

        Args:
            df (:obj:`DataFrame`): The dataframe to be manipulated.
            export_job (:obj:`ExportJob`): The export job which is currently processed.

        Returns:
            A manipulated copy of the dataframe.

        Raises:
            BoteloError: If the process of dataframe manipulation raised an exception.
        """
        logger.info("»» Manipulating collected dataframe")
        if export_job.df_manipulators:
            df = self._manipulate_df(export_job.df_manipulators, df)
        else:
            logger.info("No dataframe manipulators defined, continuing without dataframe manipulation")
        return df

    def _perform_data_export(self, df: DataFrame, export_job: ExportJob):
        """Wraps the process of exporting the data using the data exporter specified in the export job.

        Args:
            df (:obj:`DataFrame`): The dataframe to be exported.
            export_job (:obj:`ExportJob`): The export job which is currently processed.

        Raises:
            BoteloError: If the process of exporting the data raised an exception.
        """
        logger.info("»» Data export")
        if not export_job.data_exporter:
            raise BoteloError("No data exporter found in export job {0}".format(export_job.name))
        self._process_chunks(df, export_job.data_exporter, export_job.chunk_size)

    def _perform_query_saving(self, export_job: ExportJob):
        """Wraps the process of saving a query to a text file using the query saver specified in the export job.

        Args:
            export_job (:obj:`ExportJob`): The export job which is currently processed.

        Raises:
            BoteloError: If the process of saving the query raised an exception.
        """
        logger.info("»» Saving query")
        if export_job.query_saver:
            self._save_query(export_job.query_saver)
        else:
            logger.info("No query saver defined, continuing without saving the query")

    def _perform_finishing_files(self, export_job: ExportJob):
        """Wraps the process of finishing the exported files using the file finishers specified in the export job.

        Args:
            export_job (:obj:`ExportJob`): The export job which is currently processed.

        Raises:
            BoteloError: If the process of finishing the files raised an exception.
        """
        logger.info("»» Finishing exported file(s)")
        if export_job.file_finishers:
            self._finish_files(export_job.file_finishers)
        else:
            logger.info("No file finishers defined, continuing without finishing files")

    def _perform_file_packagers(self, export_job: ExportJob):
        """Wraps the process of packaging files using the file packagers specified in the export job.

        Args:
            export_job (:obj:`ExportJob`): The export job which is currently processed.

        Raises:
            BoteloError: If the process of packaging the files raised an exception.
        """
        logger.info("»» Packaging files")
        if export_job.file_packagers:
            self._package_files(export_job.file_packagers)
        else:
            logger.info("No file packagers defined, continuing without packacking files")

    def _perform_forwarders(self, export_job: ExportJob):
        """Wraps the process of forwarding files using the file forwarders specified in the export job.

        Args:
            export_job (:obj:`ExportJob`): The export job which is currently processed.

        Raises:
            BoteloError: If the process of forwarding the files raised an exception.
        """
        logger.info("»» Forwarding files")
        if export_job.file_forwarders:
            self._forward_files(export_job.file_forwarders)
        else:
            logger.info("No file forwarders defined, continuing without forwarding files")

    def _perform_cleaners(self, export_job: ExportJob):
        """Wraps the process of cleaning up using the cleaners specified in the export job.

        Args:
            export_job (:obj:`ExportJob`): The export job which is currently processed.

        Raises:
            BoteloError: If the process of cleaning up raised an exception.
        """
        logger.info("»» Cleaning up")
        if export_job.cleaners:
            self._cleanup(export_job.cleaners)
        else:
            logger.info("No cleaners defined, continuing without cleaning up")

    def _retrieve_dataframe(self, data_collector: AbstractDataCollector) -> DataFrame:
        """Retrieves a dataframe using the given data collector.

        Args:
            data_collector (:obj:`AbstractDataCollector`: An instance of AbstractDataCollector,
                most of the time the one specified in the export job.

        Returns:
            The retrieved dataframe

        Raises:
            BoteloError: If the data collector raised an exception.
            TypeError:  If the given data collector is not of type AbstractDataCollector.
        """
        if not isinstance(data_collector, AbstractDataCollector):
            raise TypeError("Given data collector is not of type AbstractDataCollector: {0}".format(type(data_collector)))

        logger.info("Trying to retrieve a dataframe using {0}...".format(data_collector.name))
        start_time = datetime.now().replace(microsecond=0)
        result = data_collector.get_dataframe(self.shared_info_collection)
        end_time = datetime.now().replace(microsecond=0)
        duration = end_time - start_time

        if isinstance(result, DataCollectorError):
            errmsg = "Retrieving the dataframe from {1} failed: {0}".format(result.message, result.data_collector.name)
            raise BoteloError(errmsg)
        else:
            df = result

        row_count = len(df)
        msg = "Successfully retrieved dataframe with {0} rows in {1}".format(row_count, duration)
        logger.info(msg)

        self._shared_info_collection['BT_DATA_COLLECTION_DURATION'] = duration
        return df

    def _gather_infos(self, info_gatherers: List[AbstractInfoGatherer]):
        """Gathers infos using the given info gatherers and populates the shared info collection with this infos.

        Args:
            info_gatherers ([:obj:`AbstractInfoGatherer`]): A list of AbstractInfoGatherer instances,
                most of the time the ones specified in the export job.

        Raises:
            BoteloError: If the process of gathering infos raised an exception.
            TypeError:  If a given info gatherer is not of type AbstractInfoGatherer.
        """
        for info_gatherer in info_gatherers:
            if not isinstance(info_gatherer, AbstractInfoGatherer):
                raise TypeError("Given info gatherer is not of type AbstractInfoGatherer: {0}".format(type(info_gatherer)))

            logger.info("Trying to gather further information using {0}...".format(info_gatherer.name))
            result = info_gatherer.get_info(self.shared_info_collection)

            if isinstance(result, dict):
                for key, value in result.items():
                    self.add_shared_info(key, value)
            elif isinstance(result, InfoGathererError):
                raise BoteloError("{0} raised an error: {1}".format(result.info_gatherer.name, result.message))

            logger.info("Successfully gathered further information".format(info_gatherer.name))

    def _manipulate_df(self, df_manipulators: List[AbstractDataFrameManipulator], df: DataFrame) -> DataFrame:
        """Manipulates a dataframe using the given manipulators.

        Args:
            df_manipulators ([:obj:`AbstractDataFrameManipulator`]): A list of AbstractDataFrameManipulator instances,
                most of the time the ones specified in the export job.

        Returns:
            A manipulated copy of the dataframe.

        Raises:
            BoteloError: If the process of dataframe manipulation raised an exception.
            TypeError:  If a given manipulator is not of type AbstractDataFrameManipulator.
        """
        cdf = df.copy()
        for df_manipulator in df_manipulators:
            if not isinstance(df_manipulator, AbstractDataFrameManipulator):
                raise TypeError("Given dataframe manipulator is not of type AbstractDataFrameManipulator: {0}".format(type(df_manipulator)))

            logger.info("Trying to manipulate dataframe using {0}...".format(df_manipulator.name))
            result = df_manipulator.manipulate(cdf, self.shared_info_collection)

            if isinstance(result, DataFrame):
                cdf = result.copy()
                # Reduces memory consumption by copied dataframes
                result = None
            elif isinstance(result, DataFrameManipulatorError):
                raise BoteloError("{0} raised an error: {1}".format(result.df_manipulator.name, result.message))

            logger.info("Successfully manipulated the collected dataframe".format(df_manipulator.name))

        return cdf

    def _process_chunks(self, df: DataFrame, data_exporter: AbstractDataExporter, chunk_size: int):
        """Exports a dataframe in chunks of a given size using the specified data exporter.

        Args:
            df (:obj:`AbstractDataExporter`): The dataframe to be exported.
            data_exporter (:obj:`AbstractDataExporter`): An instance of AbstractDataExporter, mostly the one specified in the export job.
            chunk_size (int): The maximum number of records per export file.

        Raises:
            BoteloError: If the process of exporting the dataframe raised an exception.
            TypeError:  If a given data exporter is not of type AbstractDataExporter.
        """
        if not isinstance(data_exporter, AbstractDataExporter):
            raise TypeError("Given data exporter is not of type AbstractDataExporter: {0}".format(type(data_exporter)))

        logger.info("Trying to export data using {0}...".format(data_exporter.name))

        chunk_tuples = self._get_chunk_tuples(len(df), chunk_size)
        data_exports_num = len(chunk_tuples)
        current_chunk_num = 1
        export_process_results = []

        if data_exports_num == 1:
            logger.info("Starting single data export to export {0} rows...".format(len(df)))
            chunk_index = chunk_tuples[0]
            cdf = df.iloc[chunk_index.first_row_num:chunk_index.last_row_num, :]
            process_badge = BoteloProcessBadge("E1", current_chunk_num, data_exports_num)
            export_process_result = data_exporter.export(cdf, self.shared_info_collection, process_badge)
            export_process_results.append(export_process_result)
        else:
            logger.info("Initializing a pool with {0} processes for {1} data exports...".format(self._parallel_processes, data_exports_num))
            pool = Pool(processes=self._parallel_processes)
            async_process_results = []

            for chunk_index in chunk_tuples:
                cdf = df.iloc[chunk_index.first_row_num:chunk_index.last_row_num, :]
                new_data_exporter = copy.deepcopy(data_exporter)
                process_badge = BoteloProcessBadge("E{0}".format(current_chunk_num), current_chunk_num, data_exports_num)
                async_process_result = pool.apply_async(new_data_exporter.export, args=(cdf, self.shared_info_collection, process_badge))
                async_process_results.append(async_process_result)
                current_chunk_num += 1

            pool.close()
            pool.join()

            export_process_results.extend([async_process_result.get() for async_process_result in async_process_results])

        self._check_export_process_results(export_process_results)

    def _check_export_process_results(self, export_process_results):
        """A helper function checking the export results."""
        export_errors_occured = False

        for export_process_result in export_process_results:
            if isinstance(export_process_result, DataExportResult):
                self._files_bucket.add_export_filepath(export_process_result.filepath)
            elif isinstance(export_process_result, DataExportError):
                export_errors_occured = True
                logger.error("A data export raised an error: {0}".format(export_process_result.message))

        if export_errors_occured:
            raise BoteloError("The data export encountered errors, see error messages above")

    def _save_query(self, query_saver: AbstractTextSaver):
        """Saves a query to a text file using the given query saver.

        Args:
            query_saver (:obj:`AbstractTextSaver`): An instance of AbstractTextSaver, mostly the one specified in the export job.

        Raises:
            BoteloError: If the process of saving the query raised an exception.
            TypeError:  If a given query saver is not of type AbstractTextSaver.
        """
        if not isinstance(query_saver, AbstractTextSaver):
            raise TypeError("Given query saver is not of type AbstractTextSaver: {0}".format(type(query_saver)))
        export_result = query_saver.save(self.shared_info_collection)
        if isinstance(export_result, FileSaverResult):
            self._files_bucket.query_file = export_result.filepath
        elif isinstance(export_result, FileSaverError):
            raise BoteloError("{0} raised an error: {1}".format(export_result.file_saver.name, export_result.message))

    def _finish_files(self, file_finishers: List[AbstractFileFinisher]):
        """Finishes the exported files using the given file finishers.

        Args:
            file_finishers ([:obj:`AbstractFileFinisher`]): A list of AbstractFileFinisher instances,
                mostly the ones specified in the export job.

        Raises:
            BoteloError: If the process of finishing the exported files raised an exception.
            TypeError:  If a given file finisher is not of type AbstractFileFinisher.
        """
        current_process_number = 1
        loop_filepaths = []
        loop_filepaths.extend(self._files_bucket.exported_files)

        for file_finisher in file_finishers:
            if not isinstance(file_finisher, AbstractFileFinisher):
                raise TypeError("Given file finisher is not of type AbstractFileFinisher: {0}".format(type(file_finisher)))

            logger.info("Trying to finish file using {0}...".format(file_finisher.name))

            if len(loop_filepaths) == 1:
                process_badge = BoteloProcessBadge("F{0}".format(current_process_number), current_process_number, 1)
                process_result = file_finisher.finish_file(loop_filepaths[0], self.shared_info_collection, process_badge)
                loop_filepaths = self._check_finishing_process_results([process_result])
            else:
                logger.info("»»= Finishing files with {0}".format(file_finisher.name))
                logger.info("Initializing a pool with {0} processes for {1} files...".format(self._parallel_processes, len(loop_filepaths)))
                async_process_results = []
                pool = Pool(processes=self._parallel_processes)

                for loop_filepath in loop_filepaths:
                    new_file_finisher = copy.deepcopy(file_finisher)
                    process_badge = BoteloProcessBadge("F{0}".format(current_process_number), current_process_number, len(loop_filepaths))
                    asnyc_process_result = pool.apply_async(new_file_finisher.finish_file, args=(loop_filepath, self.shared_info_collection, process_badge))
                    async_process_results.append(asnyc_process_result)
                    current_process_number += 1

                pool.close()
                pool.join()

                loop_filepaths = self._check_finishing_process_results([asnyc_process_result.get() for asnyc_process_result in async_process_results])

    def _check_finishing_process_results(self, finishing_process_results):
        """A helper function checking the file finishing results."""
        finishing_errors_occured = False
        loop_filepaths = []

        for finishing_process_result in finishing_process_results:
            if isinstance(finishing_process_result, FileFinisherResult):
                filepath = finishing_process_result.filepath
                loop_filepaths.append(filepath)
                if filepath not in self._files_bucket.finished_files:
                    self._files_bucket.add_finished_filepath(filepath)
            elif isinstance(finishing_process_result, FileFinisherError):
                finishing_errors_occured = True
                logger.error("A file finisher raised an error: {0}".format(finishing_process_result.message))

        if finishing_errors_occured:
            raise BoteloError("The finishing process encountered errors, see error messages above")

        return loop_filepaths

    def _package_files(self, file_packagers: List[AbstractFilePackager]):
        """Packages files with the given file packagers.

        Args:
            file_packagers ([:obj:`AbstractFilePackager`]): A list of AbstractFileFinisher instances,
                mostly the one specified in the export job.

        Raises:
            BoteloError: If the process of packaging files raised an exception.
            TypeError:  If a given file packager is not of type AbstractFilePackager.
        """
        loop_filepaths = self._files_bucket.get_all_filepaths()
        process_number = 1

        for file_packager in file_packagers:
            if not isinstance(file_packager, AbstractFilePackager):
                raise TypeError("Given file packager is not of type AbstractFilePackager: {0}".format(type(file_packager)))

            logger.info("Trying to package files using {0}...".format(file_packager.name))

            process_badge = BoteloProcessBadge("P{0}".format(process_number), process_number, len(file_packagers))
            file_packager_result = file_packager.package_files(loop_filepaths, self.shared_info_collection, process_badge)

            if isinstance(file_packager_result, FilePackagerResult):
                loop_filepaths = file_packager_result.filepaths
            elif isinstance(file_packager, FilePackagerError):
                raise BoteloError("{0} raised an error: {1}".format(file_packager_result.file_packager.name, file_packager_result.message))

            process_number += 1

        self._files_bucket.package_files = loop_filepaths

    def _forward_files(self, file_forwarders: List[AbstractForwarder]):
        """Forwards files with the given file forwarders.

        Args:
            file_forwarders ([:obj:`AbstractForwarder`]): A list of AbstractForwarder instances,
                mostly the one specified in the export job.

        Raises:
            BoteloError: If the process of forwarding files raised an exception.
            TypeError:  If a given file forwarder is not of type AbstractForwarder.
        """
        for file_forwarder in file_forwarders:
            if not isinstance(file_forwarder, AbstractForwarder):
                raise TypeError("Given file forwarder is not of type AbstractForwarder: {0}".format(type(file_forwarder)))

            logger.info("Trying to forward files using {0}...".format(file_forwarder.name))

            file_forwarder_result = file_forwarder.forward_files(self.files_bucket, self.shared_info_collection)
            if isinstance(file_forwarder_result, ForwarderError):
                raise BoteloError("{1} raised an error: {0}".format(file_forwarder_result.message, file_forwarder.name))

    def _cleanup(self, cleaners: List[AbstractCleaner]):
        """Cleans up with the given cleaners.

        Args:
            cleaners ([:obj:`AbstractCleaner`]): A list of AbstractCleaner instances,
                mostly the one specified in the export job.

        Raises:
            BoteloError: If the process of cleaning up raised an exception.
            TypeError:  If a given cleaner is not of type AbstractCleaner.
        """
        for cleaner in cleaners:
            if not isinstance(cleaner, AbstractCleaner):
                raise TypeError("Given file packager is not of type v: {0}".format(type(cleaner)))
            logger.info("Trying to clean up using {0}...".format(cleaner.name))
            cleaner_result = cleaner.cleanup(self.files_bucket, self.shared_info_collection)
            if isinstance(cleaner_result, CleanerError):
                raise BoteloError("{1} raised an error: {0}".format(cleaner_result.message, cleaner.name))

    def add_export_job(self, export_job: ExportJob):
        """Adds an export job to Botelo."""
        self._export_jobs.append(export_job)

    def _get_chunk_tuples(self, row_count: int, chunk_size: int) -> List[namedtuple('Index', 'first_row_num last_row_num')]:
        """Returns the index tuples for a given row count and chunk size.

        Example:
            A row_count of 20 and a chunk_size of 3 will return:
            [Index(first_row_num=0, last_row_num=3), Index(first_row_num=4, last_row_num=7), Index(first_row_num=8, last_row_num=9)]

        Args:
            row_count (int): The number of rows.
            chunk_size (int): The maximum size of one chunk.

        Returns:
            A list of tuples (namedtuple) with the indices for each chunk.

        Raises:
            ValueError: If row_count is less than 0 or chunk_size is equal to or less than 0.
        """
        chunk_tuples = []

        if row_count == 0:
            return chunk_tuples
        elif row_count < 0:
            raise ValueError("Row count must not be lower than 0 ({0})".format(row_count))

        if chunk_size <= 0:
            raise ValueError("Chunk size must not be 0 or lower than 0 ({0})".format(chunk_size))

        first_row_num = 0

        DFIndex = namedtuple('Index', 'first_row_num last_row_num')

        if row_count <= chunk_size:
            # Example:
            # row_count = 5
            # chunk_size = 10
            # => (0, 5)
            chunk_tuples.append(DFIndex(first_row_num, row_count))
        elif row_count > chunk_size:
            # Example:
            # row_count = 25
            # chunk_size = 10
            # => (0, 10)
            last_row_num = chunk_size

            while True:
                # Example:
                # row_count = 25
                # last_row_num = 10
                # 25 > 10
                if row_count > last_row_num:
                    chunk_tuples.append(DFIndex(first_row_num, last_row_num))
                elif row_count <= last_row_num:
                    # Example:
                    # row_count = 25
                    # chunk_size = 30
                    # 25 < 30
                    chunk_tuples.append(DFIndex(first_row_num, row_count))
                    break
                first_row_num += chunk_size
                last_row_num += chunk_size

        return chunk_tuples

    def _get_parallel_processes_default(self):
        """Returns the default number of parallel processes."""
        cpu_count = os.cpu_count()
        if cpu_count == 1:
            return cpu_count
        if cpu_count > 1:
            return int(cpu_count / 2)

    def _validate_parallel_processes(self, parallel_processes: int):
        """Validates a given number of parallel processes.

        Args:
            parallel_processes (int): The chosen number of parallel processes.

        Raises:
             ValueError: If the number of parallel process is equal to 0, less than 0 or equal/higher than the number of found CPUs.
        """
        cpu_count = os.cpu_count()
        if parallel_processes >= cpu_count:
            raise ValueError("The chosen number of parallel processes ({0}) is equal or higher than the number of found CPUs ({1})".format(parallel_processes, cpu_count))
        elif parallel_processes <= 0:
            raise ValueError("The chosen number of parallel processes ({0}) should be greater than 0".format(parallel_processes))
