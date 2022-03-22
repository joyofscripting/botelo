import logging
logger = logging.getLogger(__name__)


class ExportJob(object):

    def __init__(self, name="Export Job",
                 data_collector=None,
                 info_gatherers=None,
                 df_manipulators=None,
                 data_exporter=None,
                 query_saver=None,
                 file_finishers=None,
                 file_packagers=None,
                 file_forwarders=None,
                 cleaners=None,
                 chunk_size=250000,
                 shared_info_collection=None):
        if info_gatherers is None:
            info_gatherers = []

        if df_manipulators is None:
            df_manipulators = []

        if file_finishers is None:
            file_finishers = []

        if file_packagers is None:
            file_packagers = []

        if file_forwarders is None:
            file_forwarders = []

        if cleaners is None:
            cleaners = []
        if shared_info_collection is None:
            shared_info_collection = {}

        self.name = name
        self._data_collector = data_collector
        self._df_manipulators = df_manipulators
        self._info_gatherers = info_gatherers
        self._data_exporter = data_exporter
        self._query_saver = query_saver
        self._file_finishers = file_finishers
        self._file_packagers = file_packagers
        self._file_forwarders = file_forwarders
        self._cleaners = cleaners
        self._forwarders = None
        self._cleanup = None
        self._chunk_size = chunk_size
        self._shared_info_collection = shared_info_collection

    @property
    def data_collector(self):
        return self._data_collector

    @data_collector.setter
    def data_collector(self, data_collector):
        self._data_collector = data_collector

    @property
    def info_gatherers(self):
        return self._info_gatherers

    @info_gatherers.setter
    def info_gatherers(self, info_gatherers):
        self._info_gatherers = info_gatherers

    @property
    def df_manipulators(self):
        return self._df_manipulators

    @df_manipulators.setter
    def df_manipulators(self, df_manipulators):
        self._df_manipulators = df_manipulators

    @property
    def data_exporter(self):
        return self._data_exporter

    @data_exporter.setter
    def data_exporter(self, data_exporter):
        self._data_exporter = data_exporter

    @property
    def query_saver(self):
        return self._query_saver

    @query_saver.setter
    def query_saver(self, query_saver):
        self._query_saver = query_saver

    @property
    def chunk_size(self):
        return self._chunk_size

    @chunk_size.setter
    def chunk_size(self, chunk_size):
        self._chunk_size = chunk_size

    @property
    def file_finishers(self):
        return self._file_finishers

    @file_finishers.setter
    def file_finishers(self, file_finishers):
        self._file_finishers = file_finishers

    @property
    def file_packagers(self):
        return self._file_packagers

    @file_packagers.setter
    def file_packagers(self, file_packagers):
        self._file_packagers = file_packagers

    @property
    def file_forwarders(self):
        return self._file_forwarders

    @file_forwarders.setter
    def file_forwarders(self, file_forwarders):
        self._file_forwarders = file_forwarders

    @property
    def cleaners(self):
        return self._cleaners

    @cleaners.setter
    def cleaners(self, cleaners):
        self._cleaners = cleaners

    @property
    def shared_info_collection(self):
        return self._shared_info_collection

    @shared_info_collection.setter
    def shared_info_collection(self, shared_info_collection: dict):
        self._shared_info_collection = shared_info_collection

    def add_info_gatherer(self, info_gatherer):
        self._info_gatherers.append(info_gatherer)

    def add_df_manipulator(self, df_manipulator):
        self._df_manipulators.append(df_manipulator)

    def add_file_finisher(self, file_finisher):
        self._file_finishers.append(file_finisher)

    def add_file_packager(self, file_packager):
        self._file_packagers.append(file_packager)

    def add_file_forwarder(self, file_forwarder):
        self._file_forwarders.append(file_forwarder)

    def add_cleaner(self, cleaner):
        self._cleaners.append(cleaner)
