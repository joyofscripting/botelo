import unittest
import os
import math
from botelo.processors import Botelo
from botelo.jobs import ExportJob
from botelo.filesavers import QuerySaver
from botelo.collectors import TestDataCollector
from botelo.gatherers import TestInfoGatherer
from botelo.manipulators import DataFrameTimezoneStripper
from botelo.exporters import CSVDataExporter
from botelo.finishers import TestFileFinisher
from botelo.packagers import ZipFilePackager
from botelo.forwarders import TestForwarder
from botelo.cleaners import GeneralCleaner
from botelo.structures import BoteloFilesBucket

class TestBotelo(unittest.TestCase):

    def test_get_chunk_tuples(self):
        botelo = Botelo()

        chunk_tuples = botelo._get_chunk_tuples(10, 5)
        expect_chunk_tuples = [(0, 5), (5, 10)]
        self.assertListEqual(chunk_tuples, expect_chunk_tuples)

        chunk_tuples = botelo._get_chunk_tuples(10, 10)
        expect_chunk_tuples = [(0, 10)]
        self.assertListEqual(chunk_tuples, expect_chunk_tuples)

        chunk_tuples = botelo._get_chunk_tuples(11, 10)
        expect_chunk_tuples = [(0, 10), (10, 11)]
        self.assertListEqual(chunk_tuples, expect_chunk_tuples)

        chunk_tuples = botelo._get_chunk_tuples(0, 10)
        expect_chunk_tuples = []
        self.assertListEqual(chunk_tuples, expect_chunk_tuples)

        with self.assertRaises(Exception) as context:
            botelo._get_chunk_tuples(-1, 10)

        self.assertTrue('Row count must not be lower than 0' in str(context.exception))

        with self.assertRaises(Exception) as context:
            botelo._get_chunk_tuples(10, 0)

        self.assertTrue('Chunk size must not be 0 or lower than 0' in str(context.exception))

        with self.assertRaises(Exception) as context:
            botelo._get_chunk_tuples(10, -1)

        self.assertTrue('Chunk size must not be 0 or lower than 0' in str(context.exception))

    def test_set_parallel_processes(self):
        botelo = Botelo()
        cpu_count = os.cpu_count()

        with self.assertRaises(Exception) as context:
            botelo._validate_parallel_processes(cpu_count + 1)

        self.assertTrue('is equal or higher than the number of found CPUs' in str(context.exception))

        with self.assertRaises(Exception) as context:
            botelo._validate_parallel_processes(0)

        self.assertTrue('should be greater than 0' in str(context.exception))

        with self.assertRaises(Exception) as context:
            botelo._validate_parallel_processes(-1)

        self.assertTrue('should be greater than 0' in str(context.exception))

    def test_default_parallel_processes(self):
        botelo = Botelo()
        cpu_count = os.cpu_count()
        if cpu_count == 1:
            expected_parallel_processes = 1
        else:
            expected_parallel_processes = int(cpu_count / 2)

        self.assertEqual(botelo.parallel_processes, expected_parallel_processes)

    def test_files_bucket_copy(self):
        botelo = Botelo()
        botelo._files_bucket = BoteloFilesBucket()
        files_bucket = botelo.files_bucket

        self.assertIsNot(botelo._files_bucket, files_bucket)

    def test_add_shared_info(self):
        botelo = Botelo()
        botelo._shared_info_collection['test'] = 'test'

        with self.assertRaises(Exception) as context:
            botelo.add_shared_info('test', 'test')

        self.assertTrue('is already present in the shared info collection' in str(context.exception))

        botelo.add_shared_info('test2', 'test2')
        self.assertTrue(botelo.shared_info_collection['test2'] == 'test2')

    def test_full_run(self):
        shared_info_collection = {'BT_QUERY': 'select * from mytable;'}
        export_job = ExportJob(name="Test Export Job", chunk_size=255, shared_info_collection=shared_info_collection)

        test_data_collector = TestDataCollector(number_of_rows=1500, number_of_columns=15)
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

        if not 'test_key' in botelo.shared_info_collection.keys():
            raise KeyError('testkey not present in shared_info_collection')

    def test_botelo_no_export_job(self):
        botelo = Botelo()

        with self.assertRaises(Exception) as context:
            botelo.process_export_jobs()

        self.assertTrue('No export jobs found' in str(context.exception))

    def test_botelo_no_data_collector(self):
        botelo = Botelo()
        export_job = ExportJob()
        botelo.add_export_job(export_job)

        with self.assertRaises(Exception) as context:
            botelo.process_export_jobs()

        self.assertTrue('No data collector found' in str(context.exception))

    def test_botelo_no_data_exporter(self):
        botelo = Botelo()
        export_job = ExportJob()
        data_collector = TestDataCollector()
        export_job.data_collector = data_collector
        botelo.add_export_job(export_job)

        with self.assertRaises(Exception) as context:
            botelo.process_export_jobs()

        self.assertTrue('No data exporter found' in str(context.exception))

    def test_botelo_just_collector_exporter(self):
        botelo = Botelo()
        export_job = ExportJob()
        data_collector = TestDataCollector()
        export_job.data_collector = data_collector
        data_exporter = CSVDataExporter(overwrite_file=True)
        export_job.data_exporter = data_exporter
        cleaner = GeneralCleaner()
        export_job.add_cleaner(cleaner)
        botelo.add_export_job(export_job)
        botelo.process_export_jobs()

    def test_botelo_row_count(self):
        chunk_size = 1000
        number_of_rows = 5555
        last_chunk = number_of_rows % chunk_size
        count_files = math.ceil(float(number_of_rows) / float(chunk_size))

        botelo = Botelo()
        export_job = ExportJob(chunk_size=chunk_size)
        data_collector = TestDataCollector(number_of_columns=15, number_of_rows=number_of_rows)
        export_job.data_collector = data_collector
        data_exporter = CSVDataExporter(overwrite_file=True)
        export_job.data_exporter = data_exporter
        botelo.add_export_job(export_job)
        botelo.process_export_jobs()

        self.assertTrue(len(botelo.files_bucket.exported_files) == count_files)

        for filepath in botelo.files_bucket.exported_files:
            with open(filepath) as fileobj:
                lines = fileobj.readlines()
                self.assertTrue(len(lines) in (chunk_size + 1, last_chunk + 1,))
                filepath.unlink()

if __name__ == '__main__':
    unittest.main()