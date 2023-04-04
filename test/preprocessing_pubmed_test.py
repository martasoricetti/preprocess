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
from os import listdir
import pandas as pd
import glob


class PubMedPPTest(unittest.TestCase):
    """This class aims at testing the methods of the classes NIHPreProcessing and ICiteMDPreProcessing."""

    def setUp(self):
        self.test_dir = join("test", "preprocess")
        self.req_type = ".csv"
        self.num_1 = 8
        self.num_2 = 15
        self.num_4 = 4

        # iCite Metadata, for POCI glob
        self.input_md_dir = join(self.test_dir, "poci_md_pp_dump_input")
        self.output_md_dir = self.__get_output_directory("poci_md_pp_dump_output")
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



