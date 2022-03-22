from botelo.collectors import CSVFileDataCollector
from botelo.processors import Botelo
from botelo.exporters import ExcelDataExporter
from botelo.jobs import ExportJob
from pathlib import Path
import logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
logger = logging.getLogger(__name__)


def main():
    # Creating an export job with a chunk size of 250000 rows per export file
    export_job = ExportJob(chunk_size=250000)

    # Path to a very large CSV file
    csv_filepath = Path('/Users/yourname/data_files/a_very_large_data_file.csv')
    csv_data_collector = CSVFileDataCollector(csv_filepath, chunk_size=200000, csv_kwargs={'sep': ';'})
    export_job.data_collector = csv_data_collector

    excel_exporter = ExcelDataExporter(overwrite_file=True)
    export_job.data_exporter = excel_exporter

    # The shared_info_collection dictionary contains important informations for the
    # export job like the location of the export directory or the base filename for exported files
    shared_info_collection = {'BT_EXPORT_FOLDERPATH': '/Users/yourname/data_files/botelo_output/',
                              'BT_FILE_BASENAME': 'Data_ouput'}
    export_job.shared_info_collection = shared_info_collection

    botelo = Botelo()
    botelo.add_export_job(export_job)
    botelo.process_export_jobs()


if __name__ == '__main__':
    main()
