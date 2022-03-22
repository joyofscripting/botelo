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

    export_job = factory.create_pgsql_xlsx_export_job(jira_issue,
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
