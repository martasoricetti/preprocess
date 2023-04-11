#!python
# Copyright (c) 2022 The OpenCitations Index Authors.
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

import unittest
from os.path import join, exists
import os.path
from preprocessing.pubmed import NIHPreProcessing
import shutil
import csv
import os
import pandas as pd


class PubMedPPTest(unittest.TestCase):
    """This class aims at testing the methods of the classes NIHPreProcessing and ICiteMDPreProcessing."""

    def setUp(self):
        self.test_dir = join("test", "preprocess")
        self.req_type = ".csv"
        self.num_1 = 8
        self.num_2 = 15
        self.num_4 = 4

        # iCite Metadata, for POCI glob
        self.input_md_dir = join(self.test_dir, "data_poci", "csv_files")
        self.compr_input_md_dir = join(self.test_dir, "data_poci", "CSV_iCiteMD_zipped.zip")
        self.output_md_dir = self.__get_output_directory("data_poci_output")
        self.compr_output_md_dir = self.__get_output_directory("data_poci_output_compress")
        self._support_dir = "support_files"
        self.jt_path_json = join("support_files","journal_issn_ext.json")
        self.jt_path_empty = join("tmp","journal_issn_ext_temp.json")

    def __get_output_directory(self, directory):
        directory = join("", "tmp", directory)
        if not os.path.exists(directory):
            os.makedirs(directory)
        return directory

    def test_icmd_split(self):
        if exists(self.output_md_dir):
            shutil.rmtree(self.output_md_dir)
        self.assertFalse(exists(self.output_md_dir))
        self.NIHPPmd = NIHPreProcessing(self.input_md_dir, self.output_md_dir, self.num_2, self.jt_path_json, testing=True)
        self.NIHPPmd.split_input()

        # checks that the output directory is generated in the process.
        self.assertTrue(exists(self.output_md_dir))

        # checks that the input lines where stored in the correct number of files, with respect to the parameters specified.
        # checks that the number of filtered lines is equal to the number of lines in input - the number of discarded lines
        input_files, targz_fd = self.NIHPPmd.get_all_files(self.input_md_dir, self.req_type)
        len_discarded_lines = 0
        len_total_lines = 0
        for idx, file in enumerate(input_files):
            df = pd.read_csv(file, usecols=self.NIHPPmd._filter, low_memory=True)
            df.fillna("", inplace=True)
            df_dict_list = df.to_dict("records")
            len_total_lines += len(df_dict_list)
            len_discarded_lines += len([d for d in df_dict_list if not (d.get("cited_by") or d.get("references"))])

        expected_num_files = (len_total_lines - len_discarded_lines) // self.num_2 if (len_total_lines - len_discarded_lines) % self.num_2 == 0 else (
                                                                                                                                                                            len_total_lines - len_discarded_lines) // self.num_2 + 1
        files, targz_fd = self.NIHPPmd.get_all_files(self.output_md_dir, self.req_type)
        len_files = len(files)
        self.assertEqual(len_files, expected_num_files)

        len_filtered_lines = 0
        for idx, file in enumerate(files):
            with open(file, "r") as op_file:
                reader = csv.reader(op_file, delimiter=",")
                next(reader, None)
                len_filtered_lines += len(list(reader))
        self.assertEqual(len_filtered_lines, len_total_lines - len_discarded_lines)

    def test_icmd_split_compress(self):
        if exists(self.compr_output_md_dir):
            shutil.rmtree(self.compr_output_md_dir)
        self.assertFalse(exists(self.compr_output_md_dir))
        self.NIHPPmd = NIHPreProcessing(self.compr_input_md_dir, self.compr_output_md_dir, self.num_2, self.jt_path_json, testing=True)
        self.NIHPPmd.split_input()

        # checks that the output directory is generated in the process.
        self.assertTrue(exists(self.compr_output_md_dir))

        # checks that the input lines where stored in the correct number of files, with respect to the parameters specified.
        # checks that the number of filtered lines is equal to the number of lines in input - the number of discarded lines
        input_files, targz_fd = self.NIHPPmd.get_all_files(self.compr_input_md_dir, self.req_type)
        len_discarded_lines = 0
        len_total_lines = 0
        for idx, file in enumerate(input_files):
            df = pd.read_csv(file, usecols=self.NIHPPmd._filter, low_memory=True)
            df.fillna("", inplace=True)
            df_dict_list = df.to_dict("records")
            len_total_lines += len(df_dict_list)
            len_discarded_lines += len([d for d in df_dict_list if not (d.get("cited_by") or d.get("references"))])

        expected_num_files = (len_total_lines - len_discarded_lines) // self.num_2 if (len_total_lines - len_discarded_lines) % self.num_2 == 0 else (
                                                                                                                                                                            len_total_lines - len_discarded_lines) // self.num_2 + 1
        files, targz_fd = self.NIHPPmd.get_all_files(self.compr_output_md_dir, self.req_type)
        len_files = len(files)
        self.assertEqual(len_files, expected_num_files)

        len_filtered_lines = 0
        for idx, file in enumerate(files):
            with open(file, "r") as op_file:
                reader = csv.reader(op_file, delimiter=",")
                next(reader, None)
                len_filtered_lines += len(list(reader))
        self.assertEqual(len_filtered_lines, len_total_lines - len_discarded_lines)
        # remove the directory where the zst was extracted
        decompr_dir_filepath = self.compr_input_md_dir.split(".")[0] + "_decompr_zip_dir"
        if exists(decompr_dir_filepath):
            shutil.rmtree(decompr_dir_filepath)

    def test_to_validated_id_list_API(self):
        self.NIHPP_ids = NIHPreProcessing(self.input_md_dir, self.output_md_dir, self.num_2, self.jt_path_json, testing=True)

        valid_pmids = [{'id': '14198427', 'schema': 'pmid'}, {'id': '4617597', 'schema': 'pmid'}, {'id': '5656481', 'schema': 'pmid'}]
        mix_pmids = [{'id': '99111111111111', 'schema': 'pmid'}, {'id': '4617597', 'schema': 'pmid'}, {'id': '5656481', 'schema': 'pmid'}]
        invalid_pmids = [{'id': 'abc0029387741111111', 'schema': 'pmid'}, {'id': '192791729179279172917', 'schema': 'pmid'}]
        not_accepted_ids = [{'id': '2151-6065', 'schema': 'issn'}]
        valid_doi = [{'id': '10.1093/bja/47.9.1033', 'schema': 'doi'}]
        invalid_doi = [{'id': '10.1093/NOTAVALIDDOI_12345', 'schema': 'doi'}]
        outp_1 = self.NIHPP_ids.to_validated_id_list(valid_pmids, "citations")
        outp_2 = self.NIHPP_ids.to_validated_id_list(mix_pmids, "citations")
        outp_3 = self.NIHPP_ids.to_validated_id_list(invalid_pmids, "citations")
        outp_4 = self.NIHPP_ids.to_validated_id_list(not_accepted_ids, "citations")
        outp_5 = self.NIHPP_ids.to_validated_id_list(valid_doi, "citations")
        outp_6 = self.NIHPP_ids.to_validated_id_list(invalid_doi, "citations")

        self.assertEqual(outp_1, ['pmid:14198427', 'pmid:4617597', 'pmid:5656481'])

        #Note: the first pmid does not exist, but we assume it does, since iCite is the data provider of PUBMED, which
        # is the only pmid registration agency.
        self.assertEqual(outp_2, ['pmid:99111111111111', 'pmid:4617597', 'pmid:5656481'])

        #Note: None of these pmid exists, but we assume it does, since iCite is the data provider of PUBMED, which
        # is the only pmid registration agency.
        self.assertEqual(outp_3, ['pmid:29387741111111', 'pmid:192791729179279172917'])

        # This case should never happen: the id is not of an accepted schema : _accepted_ids = {"doi", "pmid"}
        self.assertEqual(outp_4, [None])

        # The doi exists
        self.assertEqual(outp_5, ['doi:10.1093/bja/47.9.1033'])

        # Check REDIS db
        #remove the doi from the redis db in case it exists
        if self.NIHPP_ids._redis_db.get("doi:10.1093/notavaliddoi_12345"):
            self.NIHPP_ids._redis_db.delete("doi:10.1093/notavaliddoi_12345")
        #check that, if the doi is not stored in redis db, it fails the API check, since it does not exist
        self.assertEqual(outp_6, [None])

    def test_to_validated_id_list_DB(self):
        self.NIHPP_ids2 = NIHPreProcessing(self.input_md_dir, self.output_md_dir, self.num_2, self.jt_path_json, testing=True)
        id_dict_list = [{'id': '10.1093/NOTAVALIDDOI_12345', 'schema': 'doi'}]
        expected = [None]
        # check id management of an id which does not exist - API
        outp = self.NIHPP_ids2.to_validated_id_list(id_dict_list, "citations")
        self.assertEqual(outp, expected)

        # check id management of the same id after it was inserted in the db (preferred validation option)
        if self.NIHPP_ids2._redis_db.get("doi:10.1093/notavaliddoi_12345"):
            self.NIHPP_ids2._redis_db.delete("doi:10.1093/notavaliddoi_12345")
        self.NIHPP_ids2._redis_db.set("doi:10.1093/notavaliddoi_12345", "meta:0000000000")
        expected2 = ["doi:10.1093/notavaliddoi_12345"]
        outp2 = self.NIHPP_ids2.to_validated_id_list(id_dict_list, "citations")
        self.assertEqual(outp2, expected2)
        self.NIHPP_ids2._redis_db.delete("doi:10.1093/notavaliddoi_12345")

    def test_splitted_to_file(self):
        if exists(self.output_md_dir):
            shutil.rmtree(self.output_md_dir)
        data_to_file = [
            {'pmid': 'pmid:2', 'doi': 'doi:10.1016/0006-291x(75)90482-9', 'title': 'Delineation of the intimate details of the backbone conformation of pyridine nucleotide coenzymes in aqueous solution.', 'authors': 'K S Bose, R H Sarma', 'year': '1975', 'journal': 'Biochemical and biophysical research communications [issn:0006-291X issn:1090-2104]', 'cited_by': 'pmid:6267127 pmid:26376 pmid:29458872 pmid:25548608 pmid:190032 pmid:22558138 pmid:26259654 pmid:31435170 pmid:28302598 pmid:21542697 pmid:26140007 pmid:890065 pmid:31544580 pmid:990401 pmid:39671', 'references': 'pmid:4150960 pmid:4356257 pmid:4846745 pmid:4357832 pmid:4683494 pmid:4414857 pmid:4337195 pmid:4747846 pmid:4266012 pmid:4147828 pmid:1111570 pmid:4366080 pmid:1133382 pmid:4364802 pmid:4709237 pmid:4733399 pmid:4379057 pmid:17742850'},
            {'pmid': 'pmid:3', 'doi': 'doi:10.1016/0006-291x(75)90498-2', 'title': 'Metal substitutions incarbonic anhydrase: a halide ion probe study.', 'authors': 'R J Smith, R G Bryant', 'year': '1975', 'journal': 'Biochemical and biophysical research communications [issn:0006-291X issn:1090-2104]', 'cited_by': 'pmid:25624746 pmid:33776281 pmid:32427033 pmid:28053241 pmid:22558138 pmid:24349293 pmid:7022113 pmid:24775716 pmid:27767123 pmid:29897055 pmid:13818 pmid:23643052 pmid:12463', 'references': 'pmid:4632671 pmid:4977579 pmid:4992430 pmid:4958988 pmid:14417215 pmid:4992429 pmid:4621826 pmid:804171 pmid:4263471 pmid:4625501 pmid:4399045 pmid:4987292 pmid:4628675'}]

        self.NIHPP_files= NIHPreProcessing(self.input_md_dir, self.output_md_dir, 1,  self.jt_path_json, testing=True)
        count = 0
        for el in data_to_file:
            count += 1
            reduced_list = [el]
            self.NIHPP_files.splitted_to_file(count, reduced_list, ".csv")

        output_file_n = len(os.listdir(self.output_md_dir))
        expected_n = 2
        self.assertEqual(output_file_n, expected_n)

        all_dict_in_out = []
        all_fi, op = self.NIHPP_files.get_all_files(self.output_md_dir, ".csv")
        for fi in all_fi:
            myfi = open(fi, 'r')
            reader = csv.DictReader(myfi)
            for dictionary in reader:
                all_dict_in_out.append(dictionary)
            myfi.close()
        self.assertTrue(all(item in data_to_file for item in all_dict_in_out))
        self.assertTrue(all(item in all_dict_in_out for item in data_to_file))
