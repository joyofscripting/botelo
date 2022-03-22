
from botelo.collectors import TestDataCollector
from botelo.gatherers import TestInfoGatherer
from botelo.manipulators import DataFrameTimezoneStripper
from botelo.exporters import CSVDataExporter
from botelo.filesavers import QuerySaver
from botelo.finishers import TestFileFinisher
from botelo.packagers import ZipFilePackager
from botelo.forwarders import TestForwarder
from botelo.cleaners import GeneralCleaner
from botelo.jobs import ExportJob
from botelo.processors import Botelo

import logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
logger = logging.getLogger(__name__)


def main():
    sql_query = 'select id from my_table mt;'
    shared_info_collection = {'BT_QUERY': sql_query}

    export_job = ExportJob(name="Test Export Job", chunk_size=10000, shared_info_collection=shared_info_collection)

    test_data_collector = TestDataCollector(number_of_rows=545635, number_of_columns=15)
    export_job.data_collector = test_data_collector
    info_gatherer = TestInfoGatherer()
    export_job.add_info_gatherer(info_gatherer)
    df_manipulator = DataFrameTimezoneStripper()
    export_job.add_df_manipulator(df_manipulator)
    csv_data_exporter = CSVDataExporter(overwrite_file=True)
    export_job.data_exporter = csv_data_exporter
    file_finisher = TestFileFinisher(overwrite_file=True)
    export_job.add_file_finisher(file_finisher)
    zip_packager = ZipFilePackager(overwrite_file=True)
    export_job.add_file_packager(zip_packager)
    forwarder = TestForwarder()
    export_job.add_file_forwarder(forwarder)
    cleaner = GeneralCleaner()
    export_job.add_cleaner(cleaner)

    query_saver = QuerySaver('.sql')
    export_job.query_saver = query_saver

    botelo = Botelo()
    botelo.add_export_job(export_job)
    botelo.process_export_jobs()


if __name__ == '__main__':
    main()
