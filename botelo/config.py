import botelo.secrets
import os
import sys


NAMED_CONNECTIONS = {
    'PostgreSQL PROD': {
        'host': 'db.megadata.local',
        'dbname': 'zaphod',
        'user': 'beeblebrox',
        'password': botelo.secrets.DB_PASSWORDS['beeblebrox'],
        'application_name': 'botelo'
    },
    'Apache Drill Local': {
        'host': 'localhost',
        'port': 8047,
        'timeout': 10
    }
}

CURRENT_FOLDER_PATH = os.path.dirname(sys.argv[0])
EXPORT_FOLDERPATH = '/Users/yourname/Desktop/botelo/'
DEFAULT_EXPORT_FOLDERPATH = os.path.join(CURRENT_FOLDER_PATH, 'data_files')
DEFAULT_FILE_BASENAME = 'Data_output'

EXCEL_WRITER_KWARGS = {'engine': 'xlsxwriter', 'engine_kwargs': {'options': {'encoding': 'utf-8', 'remove_timezone': True, 'strings_to_formulas': False}}}
EXCEL_DATAFRAME_KWARGS = {'index': False, 'sheet_name': 'Data'}

JIRA_BASE_URL = 'https://jira.yourdomain.com'
JIRA_USER = 'zaphod'
JIRA_PASSWORD = botelo.secrets.JIRA_PASSWORDS[JIRA_USER]
