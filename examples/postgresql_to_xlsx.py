from botelo import config
from botelo import secrets
from botelo.processors import Botelo
from botelo.jobs import ExportJob
from botelo.gatherers import JiraIssueTitleGatherer
from botelo.gatherers import FormattedDatetimeGatherer
from botelo.gatherers import BaseFilenameGatherer
from botelo.manipulators import DataFrameTimezoneStripper
from botelo.collectors import PostgreSQLDataCollector
from botelo.exporters import ExcelDataExporter
from botelo.finishers import ExcelDecorator
from botelo.filesavers import QuerySaver
from botelo.packagers import ZipFilePackager
from botelo.forwarders import JiraForwarder
from botelo.cleaners import GeneralCleaner

import logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
logger = logging.getLogger(__name__)


def main():
    # This is the SQL query to be used to fetch data from the PostgreSQL database
    sql_statement = """select * from my_schema.data_table dat where dat.data_type_id = 42;"""
    # This is the name of the database connection to be used (defined in config.py)
    connection_name = 'PROD'
    # This is the code of the Jira issue where the created Excel files will be uploaded
    jira_issue = 'DAT-4242'
    # This is the name of the creator who executes this export job (will be shown in the created Excel files)
    creator_name = 'Zaphod Beeblebrox'
    # This is the name of the export job
    job_name = 'Data Export DAT-4242'

    connection_settings = config.NAMED_CONNECTIONS[connection_name]
    # The shared_info_collection dictionary contains important informations for the
    # export job like the location of the export directory or the base filename for exported files
    shared_info_collection = {'BT_EXPORT_FOLDERPATH': config.EXPORT_FOLDERPATH,
                              'jira_issue': jira_issue,
                              'jira_base_url': config.JIRA_BASE_URL,
                              'jira_url': "https://{0}/browse/{1}".format(config.JIRA_BASE_URL, jira_issue),
                              'jira_user': config.JIRA_USER,
                              'jira_password': secrets.JIRA_PASSWORDS[config.JIRA_USER],
                              'connection_name': connection_name.replace(' ', '_'),
                              'db_host': connection_settings['host'],
                              'db_name': connection_settings['dbname'],
                              'creator': creator_name,
                              'BT_QUERY': sql_statement}

    # By setting the argument chunk_size to the value 250000 the export job will restrict every
    # created Excel file to contain a maximum of 250000 rows each
    export_job = ExportJob(name=job_name, chunk_size=250000, shared_info_collection=shared_info_collection)

    # This info gatherer fetches the title of the Jira issue and saves it in the
    # shared info collection under the key 'jira_issue_title'
    jit_gatherer = JiraIssueTitleGatherer('jira_issue_title')
    export_job.add_info_gatherer(jit_gatherer)

    # This info gatherer fetches the current date and time and saves it in the
    # shared info collection under the key 'formatted_datetime'
    formatted_date_gatherer = FormattedDatetimeGatherer('formatted_datetime', '%Y%m%d_%H%M%S')
    export_job.add_info_gatherer(formatted_date_gatherer)

    # This info gatherer creates the base filename that will be used for the
    # exported Excel files
    formatstr = 'Data_export_{formatted_datetime}_{connection_name}_{jira_issue}'
    file_basename_gatherer = BaseFilenameGatherer('BT_FILE_BASENAME', formatstr)
    export_job.add_info_gatherer(file_basename_gatherer)

    # Excel sometimes has problems with timezones. So as a precautionary
    # measure we strip them from the dataframe with this manipulator
    df_tz_stripper = DataFrameTimezoneStripper()
    export_job.add_df_manipulator(df_tz_stripper)

    # To fetch the data from the PostgreSQL database this data collector is used
    data_collector = PostgreSQLDataCollector()
    data_collector.connection_settings = connection_settings
    data_collector.sql_statement = sql_statement
    # The new psycopg version cannot handle multiple SQL commands
    # in one statement. So we have to use pre SQL statements which
    # are executed before the main SQL statement
    work_mem_size = 2
    pre_sql_statement = "set work_mem = '{0}GB';".format(work_mem_size)
    data_collector.pre_sql_statements = [pre_sql_statement]
    export_job.data_collector = data_collector

    # This is the exporter creating Excel files
    data_exporter = ExcelDataExporter(ew_kwargs=config.EXCEL_WRITER_KWARGS, df_kwargs=config.EXCEL_DATAFRAME_KWARGS)
    export_job.data_exporter = data_exporter

    # Used to save the SQL statement to a text file for documentation purposes
    query_saver = QuerySaver('.sql')
    export_job.query_saver = query_saver

    # This object helps with enhancing the generated Excel files,
    # e.g. adding a worksheet containing the SQL query
    excel_decorator = ExcelDecorator()
    export_job.add_file_finisher(excel_decorator)

    # This forwarder uploads files of interest to the Jira issue and adds a corresponding comment
    comment = 'botelo uploaded {count_files} file(s) to this Jira issue: \n\n{filenames}'
    jira_forwarder = JiraForwarder(suffix_filter=['.sql', '.xlsx'], jira_comment=comment)
    export_job.add_file_forwarder(jira_forwarder)

    # Creates a ZIP archive of the created files
    zip_archiver = ZipFilePackager()
    export_job.add_file_packager(zip_archiver)

    # Cleans up the mess we created, hence removes the XLSX/SQL files, but not the ZIP archive
    cleaner = GeneralCleaner(suffix_filter=['.xlsx', '.sql'])
    export_job.add_cleaner(cleaner)

    # Let's create an instance of botelo, add our add export job and start processing
    botelo = Botelo()
    botelo.add_export_job(export_job)

    botelo.process_export_jobs()


if __name__ == '__main__':
    main()
