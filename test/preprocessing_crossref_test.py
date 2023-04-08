import unittest
import json
from preprocessing.crossref import CrossrefPreProcessing
from os.path import exists, join
import os.path
import shutil

class PreprocessingTestCrossref(unittest.TestCase):
    def setUp(self):
        self.test_dir = join("test", "preprocess", "data_crossref")
        self.decompr_input = join(self.test_dir, "json_files")
        self.compr_input = join(self.test_dir, "crossref_sample.tar.gz")
        self._output_dir_cr = self.__get_output_directory("data_crossref")
        self._output_dir_cr_compr = self.__get_output_directory("data_crossref_compress")
        self._interval = 7

    def __get_output_directory(self, directory):
        directory = join("", "tmp", directory)
        if not os.path.exists(directory):
            os.makedirs(directory)
        return directory

    def test_cr_preprocessing(self):
        if exists(self._output_dir_cr):
            shutil.rmtree(self._output_dir_cr)
        self._dc_pp = CrossrefPreProcessing(self.decompr_input, self._output_dir_cr, self._interval, testing=True)
        self._dc_pp.split_input()
        entities_w_citations = []
        all_files, targz_fd = self._dc_pp.get_all_files(self.decompr_input, self._dc_pp._req_type)
        for file_idx, file in enumerate(all_files, 1):
            f = open(file, encoding="utf8")
            dict_loaded = json.load(f)
            lines = [line for line in dict_loaded["items"] if line.get("DOI") and line.get("reference")]
            entities_w_citations.extend(lines)

            f.close()
            n_ents_w_cit = len(entities_w_citations)

            n_out_ents = 0
            all_files_out, targz_fd = self._dc_pp.get_all_files(self._output_dir_cr, self._dc_pp._req_type)
            for file_idx, file in enumerate(all_files_out, 1):
                fo = open(file, encoding="utf8")
                dict_loaded_out = json.load(fo)
                lines_out = [line for line in dict_loaded_out["items"] if line.get("DOI") and line.get("reference")]
                n_out_ents += len(lines_out)
                fo.close()

            # TESTING THAT: the number of filtered entities in output is the same as
            # the n of input entities having citations
            self.assertEqual(n_out_ents, n_ents_w_cit)

            # TESTING THAT: the number of output entities is organized in the correct amount of output
            # files, according to the specified number of entities to store in each output file
            if n_ents_w_cit % self._interval == 0:
                exp_n_out_file = n_ents_w_cit // self._interval
            else:
                exp_n_out_file = (n_ents_w_cit // self._interval) + 1

            len_out_files = len([name for name in os.listdir(self._output_dir_cr) if
                                 os.path.isfile(os.path.join(self._output_dir_cr, name))])

            self.assertEqual(exp_n_out_file, len_out_files)

    def test_cr_preprocessing_compress_input(self):
        if exists(self._output_dir_cr_compr):
            shutil.rmtree(self._output_dir_cr_compr)
        self._dc_pp = CrossrefPreProcessing(self.compr_input, self._output_dir_cr_compr, self._interval, testing=True)
        self._dc_pp.split_input()
        entities_w_citations = []
        all_files, targz_fd = self._dc_pp.get_all_files(self.compr_input, self._dc_pp._req_type)
        for file_idx, file in enumerate(all_files, 1):
            dict_loaded = self._dc_pp.load_json(file, targz_fd)
            lines = [line for line in dict_loaded["items"] if line.get("DOI") and line.get("reference")]
            entities_w_citations.extend(lines)

            n_ents_w_cit = len(entities_w_citations)

            n_out_ents = 0
            all_files_out, targz_fd = self._dc_pp.get_all_files(self._output_dir_cr_compr, self._dc_pp._req_type)
            for file_idx, file in enumerate(all_files_out, 1):
                fo = open(file, encoding="utf8")
                dict_loaded_out = json.load(fo)
                lines_out = [line for line in dict_loaded_out["items"] if line.get("DOI") and line.get("reference")]
                n_out_ents += len(lines_out)
                fo.close()

            # TESTING THAT: the number of filtered entities in output is the same as
            # the n of input entities having citations
            self.assertEqual(n_out_ents, n_ents_w_cit)

            # TESTING THAT: the number of output entities is organized in the correct amount of output
            # files, according to the specified number of entities to store in each output file
            if n_ents_w_cit % self._interval == 0:
                exp_n_out_file = n_ents_w_cit // self._interval
            else:
                exp_n_out_file = (n_ents_w_cit // self._interval) + 1

            len_out_files = len([name for name in os.listdir(self._output_dir_cr_compr) if
                                 os.path.isfile(os.path.join(self._output_dir_cr_compr, name))])

            self.assertEqual(exp_n_out_file, len_out_files)
if __name__ == '__main__':
    unittest.main()
