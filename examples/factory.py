from botelo.jobs import ExportJob
from botelo.gatherers import JiraIssueTitleGatherer
from botelo.gatherers import FormattedDatetimeGatherer
from botelo.gatherers import BaseFilenameGatherer
from botelo.manipulators import DataFrameTimezoneStripper
from botelo.collectors import PostgreSQLDataCollector
from botelo.exporters import ExcelDataExporter
from botelo.exporters import CSVDataExporter
from botelo.filesavers import QuerySaver
from botelo.finishers import ExcelDecorator
from botelo.packagers import ZipFilePackager
from botelo.packagers import TarFilePackager
from botelo.forwarders import JiraForwarder
from botelo.cleaners import GeneralCleaner
from botelo import config

def create_pgsql_xlsx_export_job(job_name,
                               chunk_size,
                               shared_info_collection,
                               connection_settings,
                               sql_statement,
                               filename_tag,
                               work_mem_size):
    export_job = ExportJob(name=job_name, chunk_size=chunk_size, shared_info_collection=shared_info_collection)

    jit_gatherer = JiraIssueTitleGatherer('jira_issue_title')
    export_job.add_info_gatherer(jit_gatherer)

    formatted_date_gatherer = FormattedDatetimeGatherer('formatted_datetime', '%Y%m%d_%H%M%S')
    export_job.add_info_gatherer(formatted_date_gatherer)
    if not filename_tag:
        formatstr = 'Data_export_{formatted_datetime}_{connection_name}_{jira_issue}'
    else:
        formatstr = 'Data_export_' + filename_tag + '_{formatted_datetime}_{connection_name}_{jira_issue}'
    file_basename_gatherer = BaseFilenameGatherer('BT_FILE_BASENAME', formatstr)
    export_job.add_info_gatherer(file_basename_gatherer)

    df_tz_stripper = DataFrameTimezoneStripper()
    export_job.add_df_manipulator(df_tz_stripper)

    data_collector = PostgreSQLDataCollector()
    data_collector.connection_settings = connection_settings
    data_collector.sql_statement = sql_statement
    pre_sql_statement = "set work_mem = '{0}GB';".format(work_mem_size)
    data_collector.pre_sql_statements = [pre_sql_statement]
    export_job.data_collector = data_collector

    data_exporter = ExcelDataExporter(ew_kwargs=config.EXCEL_WRITER_KWARGS, df_kwargs=config.EXCEL_DATAFRAME_KWARGS)
    export_job.data_exporter = data_exporter

    query_saver = QuerySaver('.sql')
    export_job.query_saver = query_saver

    excel_decorator = ExcelDecorator()
    export_job.add_file_finisher(excel_decorator)

    comment = 'botelo uploaded {count_files} file(s) to this Jira issue: \n\n{filenames}'
    jira_forwarder = JiraForwarder(suffix_filter=['.sql', '.xlsx'], jira_comment=comment)
    export_job.add_file_forwarder(jira_forwarder)

    zip_archiver = ZipFilePackager()
    export_job.add_file_packager(zip_archiver)

    cleaner = GeneralCleaner(suffix_filter=['.xlsx', '.sql'])
    export_job.add_cleaner(cleaner)

    return export_job

def create_pgsql_csv_export_job(job_name,
                               chunk_size,
                               shared_info_collection,
                               connection_settings,
                               sql_statement,
                               filename_tag,
                               work_mem_size):
    export_job = ExportJob(name=job_name, chunk_size=chunk_size, shared_info_collection=shared_info_collection)

    jit_gatherer = JiraIssueTitleGatherer('jira_issue_title')
    export_job.add_info_gatherer(jit_gatherer)

    formatted_date_gatherer = FormattedDatetimeGatherer('formatted_datetime', '%Y%m%d_%H%M%S')
    export_job.add_info_gatherer(formatted_date_gatherer)
    if not filename_tag:
        formatstr = 'Data_export_{formatted_datetime}_{connection_name}_{jira_issue}'
    else:
        formatstr = 'Data_export_' + filename_tag + '_{formatted_datetime}_{connection_name}_{jira_issue}'
    file_basename_gatherer = BaseFilenameGatherer('BT_FILE_BASENAME', formatstr)
    export_job.add_info_gatherer(file_basename_gatherer)

    df_tz_stripper = DataFrameTimezoneStripper()
    export_job.add_df_manipulator(df_tz_stripper)

    data_collector = PostgreSQLDataCollector()
    data_collector.connection_settings = connection_settings
    data_collector.sql_statement = sql_statement
    pre_sql_statement = "set work_mem = '{0}GB';".format(work_mem_size)
    data_collector.pre_sql_statements = [pre_sql_statement]
    export_job.data_collector = data_collector

    data_exporter = CSVDataExporter(overwrite_file=True, df_kwargs={'sep': ';', 'index': False})
    export_job.data_exporter = data_exporter

    query_saver = QuerySaver('.sql')
    export_job.query_saver = query_saver

    comment = 'botelo uploaded {count_files} file(s) to this Jira issue: \n\n{filenames}'
    jira_forwarder = JiraForwarder(suffix_filter=['.sql', '.csv'], jira_comment=comment)
    export_job.add_file_forwarder(jira_forwarder)

    zip_archiver = ZipFilePackager()
    export_job.add_file_packager(zip_archiver)

    cleaner = GeneralCleaner(suffix_filter=['.csv', '.sql'])
    export_job.add_cleaner(cleaner)

    return export_job