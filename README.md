# botelo ⛽️
***

botelo helps to quickly export a pandas DataFrame to a certain format and to further process, package and forward the exported files.

***

botelo currently supports:

* Python 3.8.9 (and higher)
* psycopg 3.0.8 (and higher)

## Background
I initially developed botelo to automate my tedious task of:

* Fetching data from a database with an SQL query
* Creating one or more Excel files with the retrieved data
* Enhancing the Excel file(s) with further informations (e.g. duration of the SQL query, Jira issue)
* Uploading the Excel file(s) to Jira and writing a corresponding comment
* Cleaning up the local environment (deleting/archiving generated Excel file(s))

With botelo you can create a Python script with a configured export job that does all that automatically. And if your colleagues surprisingly decide that they rather need the data export in CSV or Parquet format you can simply swap the data exporter and quickly deliver the files.

botelo is very quick in exporting as it uses parallel processes to create export files.

## Quick start

### Examples
#### Converting a large CSV file to several smaller Excel files
In its most simple form a botelo job just collects data and exports it into a different format. Here is an example script that converts a large CSV file into smaller Excel files with a row limit of 250000 rows:

```
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

```
#### Retrieving data from a PostgreSQL database, exporting it to Excel and uploading the Excel files to Jira
```
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

```
#### Using an export job factory to reduce the amount of boilerplate code
In order to reduce the amount of boilerplate code to just create the export job I often use a export job factory. In combination with a command line Python script this makes for a great workflow:

Python script named factory.py containing methods returning different export jobs:

```
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
```

Python script with a command line interface (CLI) to run jobs directly from the Terminal:

```
from argparse import ArgumentParser
from pathlib import Path
import sys
import botelo.config as config
from botelo import secrets
import factory
from botelo.processors import Botelo

import logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
logger = logging.getLogger(__name__)


def export_data(jira_issue, connection_name, chunk_size, query, creator_name, filename_tag, work_mem_size):
    sql_statement = query

    connection_settings = config.NAMED_CONNECTIONS[connection_name]
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

    export_job = factory.create_gvl_xlsx_export_job(jira_issue,
                                                    chunk_size,
                                                    shared_info_collection,
                                                    connection_settings,
                                                    sql_statement,
                                                    filename_tag,
                                                    work_mem_size)

    botelo = Botelo()
    botelo.add_export_job(export_job)

    botelo.process_export_jobs()


def get_chosen_options(args):
    parser = ArgumentParser()
    parser.add_argument("-c", "--chunk_size", dest="chunk_size", type=int, default=250000, required=False, help="count of data records allowed per export file")
    parser.add_argument("-j", "--jira_issue", dest="jira_issue", required=True, help="Jira issue")
    parser.add_argument("-q", "--query_file", dest="query_filepath", required=True, help="Path to the file containing the query")
    parser.add_argument("-n", "--connection_name", dest="connection_name", required=True, help="name of the data connection to use")
    parser.add_argument("-u", "--creator_name", dest="creator_name", default='Your name', required=False, help="name of the data export creator")
    parser.add_argument("-t", "--filename_tag", dest="filename_tag", default='', required=False, help="a tag for the filename")
    parser.add_argument("-w", "--work_mem_size", dest="work_mem_size", default='2', required=False, help="work mem size for PostgreSQL")
    chosen_options = parser.parse_args(args)
    return chosen_options


def main(args):
    chosen_options = get_chosen_options(args)

    if chosen_options.connection_name not in config.NAMED_CONNECTIONS.keys():
        raise KeyError("The given connection name is not present in the configuration: {0}".format(chosen_options.connection_name))

    query_path = Path(chosen_options.query_filepath)
    if not query_path.exists():
        error_message = "The given query file does not exist: {0}".format(chosen_options.query_filepath)
        raise FileNotFoundError(error_message)
    elif not query_path.is_file():
        error_message = "The given query file does not point to a file: {0}".format(chosen_options.query_filepath)
        raise FileNotFoundError(error_message)
    else:
        with open(chosen_options.query_filepath) as query_file:
            query = query_file.read()

    export_data(chosen_options.jira_issue, chosen_options.connection_name, chosen_options.chunk_size, query, chosen_options.creator_name, chosen_options.filename_tag, chosen_options.work_mem_size)


if __name__ == '__main__':
    main(sys.argv[1:])

```
Now running different exports is quick & easy:

````
python /Users/yourname/afolder/botelo/cli_xlsx_export.py -n 'Customer DB PROD' -q '/Users/yourname/afolder/query.sql' -j 'DAT-42'
````

## Configuration

### config.py
To customize botelo to your own needs you can tweak the options in the config.py file:

* NAMED_CONNECTIONS: A dictionary to hold different connection settings for PostgreSQL, Apache Drill etc.
* EXPORT_FOLDERPATH: The directory where botelo will save exported data files
* JIRA\_BASE_\URL: The URL of Jira (when you want to upload files automatically)
* JIRA_USER: The name of the Jira user
* JIRA_PASSWORD: The password of the Jira user

#### secrets.py

The secrets.py contains the different password needed by botelo. You can use a different approach (like using environment variables). This one just works best for me currently.


## Processing steps of a single botelo job

botelo itself can process many jobs. It processes them in order and not in parallel. A single botelo job contains the following process steps with many being optional:

1. **Data collection**: This step retrieves a dataframe from a data source of your choice (database, file, already existing dataframe).
2. **Information gathering** (optional): Further information for subsequent steps is gathered. Like getting the title of a Jira issue or obtaining a formatted date string needed for the base filename.
3. **Dataframe manipulation** (optional): During this step the aquired dataframe can be manipulated before it gets exported.
4. **Data export** (parallel): This step exports the dataframe to export files of a desired format. You can choose a chunk size and to speed up the process exporting files is done in parallel.
5. **File finishing** (optional, parallel): After the data export files are created botelo can tune them further. This might include adding worksheets to an Excel file or creating a ZIP archive for a CSV file to reduce filesize.
6. **File packaging** (optional): If your botelo job created several files you might want to archive them as a package. This step can automate this task.
7. **File forwarding** (optional): Very often you might want to share the created files with your colleagues. In this step you can move chosen files to a certain directory, upload them to a Jira issue or save them on a FTP server.
8. **Cleaning up** (optional): In this last housekeeping step you can remove created files to keep your export directory clean and tidy.

The most simple botelo job will just collect data and export it quickly to a different format. But you can also use a botelo job to automate more complex workflows.


## Documentation
### shared\_info\_collection
The shared\_info\_collection is a dictionary that contains information used and needed by several elements of a single botelo job.

You can add your own information, but there are some unique keys that can be used to control & customize a botelo export job:

#### BT_QUERY
This key holds the information about the query that you would like to use with the data collector you chose. A data collector that needs a query to fetch data will look for this key in the shared\_info\_collection dictionary and use the value as the query statement.

Example:
```
shared_info_collection['BT_QUERY'] = "select * from my_table mt where mt.customer_id = %s;"
```

#### BT\_QUERY\_PARAMETERS
If you need to use a prepared statement you can use the key BT\_QUERY\_PARAMETERS to save the query parameters. A data collector that supports prepared statements (like the PostgreSQLDataCollector) will fetch the query parameters from the shared_info_collection with this key.

Example:
```
shared_info_collection['BT_QUERY_PARAMETERS'] = (1092, 1876, 9087)
```

#### BT\_DATA\_COLLECTION_DURATION
After the step of data collection is finished botelo will publish the duration this step took with the key BT\_DATA\_COLLECTION_DURATION in the shared\_info\_collection dictionary. You can then access it and use this information in the subsequent steps.

#### BT\_FILE\_BASENAME
This key hold the information about the so called base filename. If you do not provide it in the shared\_info\_collection dictionary botelo will default to the settings in the configuration (config.py).

Examples:
```
shared_info_collection['BT_FILE_BASENAME'] = "Data_export_product_data_202201"
```
Following this example the Excel exporter would generate the following filenames for saving the files:

Single file:

* Data\_export\_product\_data\_202201.xlsx

Mutiple files:

1. Data\_export\_product\_data\_202201\_1.xlsx
2. Data\_export\_product\_data\_202201\_2.xlsx
3. Data\_export\_product\_data\_202201\_3.xlsx

#### BT\_EXPORT\_FOLDERPATH
This key specifies the directory where the export job will save its files. If you do not provide it in the shared\_info\_collection dictionary botelo will default to the settings in the configuration (config.py).

```
shared_info_collection['BT_EXPORT_FOLDERPATH'] = "/Users/yourname/botelo/data_exports/"
```

### Data collectors
A data collector is used in the first step of a botelo job. It will collect data and return it as a dataframe.

You can easily create your own data collectors by subclassing the abstract base class AbstractDataCollector. But botelo already comes with some useful data collectors included:

#### PassThroughDataFrameCollector
This data collector comes in handy when you already created a dataframe and just want to export it to a certain data format. It follows the AbstractDataCollector class, but will just return the dataframe you passed it during initialization.

#### PostgreSQLDataCollector
This data collector connects to a PostgreSQL database, fetches data with a query and returns it as a dataframe.

#### ApacheDrillDataCollector
This data collector connects to an Apache Drill instance, fetches data with a query and returns it as a dataframe.

#### CSVFileDataCollector
If you need to read data from a CSV file this data collector will be useful. 

### Information gatherers
In a botelo job information gatherers are processed after the data collection step is finished.

An information gatherer helps to create small information pieces that can be used in the subsequent steps of the current botelo job. For example, retrieving a formatted date string that can be used in filenames. Or retrieving the title of a certain Jira issue to include this information in an Excel file to be exported.

Please note that information gatherers are processed in order: The first information gatherer that you added to a botelo job will also be the first to be processed. In this way you can use information from the shared\_info\_collection dictionary that was gathered and published by a previous information gatherer. Keep this in mind when you need to chain information gatherers.

You can create your own information gatherers by subclassing the abstract base class AbstractInfoGatherer. The following information gatherers are already available:

#### JiraIssueTitleGatherer
If you don't want to manually copy the Jira issue title from your webbrowser the JiraIssueTitleGatherer can fetch it for you. I wrote it, because my colleagues always wanted to have the Jira issue title to be included as an information in the corresponding Exce files.

#### FormattedDatetimeGatherer
This information gatherer will generate a formatted date string and save it with the key of your choice in the shared\_info\_collection dictionary.

#### BaseFilenameGatherer
A very useful info gatherer that can create a custom base filename with informations from the shared\_info\_dictionary by using a format string.

Example:

(In this example we use a FormattedDatetimeGatherer to get a formatted date string which we then use in the BaseFilenameGatherer. This way of chaining info gatherers can be very useful.)

```
formatted_date_gatherer = FormattedDatetimeGatherer('formatted_datetime', '%Y%m%d_%H%M%S')
export_job.add_info_gatherer(formatted_date_gatherer)
formatstr = 'Data_export_{formatted_datetime}_{connection_name}_{jira_issue}_V01'
file_basename_gatherer = BaseFilenameGatherer('BT_FILE_BASENAME', formatstr)
export_job.add_info_gatherer(file_basename_gatherer)
```
Depending on the values the result could be something like:

```
>>> shared_info_collection['BT_FILE_BASENAME']
'Data_export_20220115_173543_MyProdDB_DEV-829_V01'
```

### Dataframe manipulators
Once you collected the data with the data collector you might want to manipulate, clean or transform this data before exporting it. In a botelo job this can be done with dataframe manipulators.

You can write your own dataframe manipulators by subclassing the abstract base class AbstractDataFrameManipulator. Currently the following dataframe manipulators are included:

#### DataFrameTimezoneStripper
I encountered many problems when trying to export dates with timezones to Excel files. That is why I wrote this manipulator which tries to strip the timezone information from dates in a dataframe.

### Data exporters
Data exporters are at the heart of botelo and will export the retrieved dataframe to the desired format in chunks of your choice. Data exports are always processed in parallel to speed up the process. If you don't specify a maximum of parallel processed, botelo will use half of the available CPU cores.

You can write your own data exporters by subclassing the abstract base class AbstractDataExporter. But botelo also comes with several data exporters already included:

#### ExcelDataExporter
This data exporter exports the retrieved dataframe as Excel files.

#### CSVDataExporter
This data exporter exports the retrieved dataframe as CSV files.

#### ParquetDataExporter
This data exporter exports the retrieved dataframe as Parquet files.

### Query savers
If you want to save the underlying query of your data export into a separate file for documentation purposes you can use a query saver.

You can write your own query savers by subclassing the anstract base class AbstractTextSaver. Nevertheless the included query saver will be sufficient in many circumstances:

#### QuerySaver
Saves the query in a text file using UTF-8 encoding.

### File finishers
After the data exporter of a botelo job created the export files, file finishers can further tune and process them. A typical case for a file finisher is to furnish an Excel file with additional worksheets or to create a ZIP archive for a CSV file to shrink its file size.

You can write your own fle finishers by subclassing the abstract base class AbstractFileFinisher. These file finishers are already available in botelo:

#### ZipFileFinisher
This file finisher creates a ZIP archive for an exported data file.

#### ExcelDecorator
This file finisher decorates an exported Excel file with additional worksheets.

### File packagers
If your botelo job created several data export files you might want to save them into a single archive. This is where file packagers can help you to automate this step. File finishers have access to all files created in previous steps of the botelo job and can create custom archives.

You can write your own fle packagers by subclassing the abstract base class AbstractFilePackager. The following file packagers are already included:

#### ZipFilePackager
This file packager can create a ZIP archive from serveral files.

#### TarFilePackager
This file packager can create a TAR archive (*.tar.gz) from serveral files.

### File forwarders
Once you exported, optionally finished and packaged your data files you might want to forward them to share them with your colleagues. For this step file forwarders are used in a botelo job. They can upload your files to a Jira issue, move them to a different directory or save them on a FTP server.

You can write your own fle packagers by subclassing the abstract base class AbstractForwarder. These file forwarders are included so far in botelo:

#### JiraForwarder
This file forwarder will upload files of your choise to a specific Jira issue. If you want it can also conveniently create a summarizing comment about the uploaded files.

### Cleaners
The last step of a botelo job is cleaning up. With a cleaner you can remove created files to save your export directory from getting a mess. Housekeeping is necessary and cleaners are an efficient way to automate it.

You can write your own fle packagers by subclassing the abstract base class AbstractCleaner. These cleaners can be readily used in botelo:

#### GeneralCleaner
This file cleaner removes files created during a botelo job. You can exclude files from being removed with a suffix filter.