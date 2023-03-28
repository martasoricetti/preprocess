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

import json
import unittest
from preprocessing.datacite import DatacitePreProcessing
from os.path import exists, join
import os.path
import shutil


class PreprocessingTest(unittest.TestCase):
        def setUp(self):
            self.test_dir = join("test", "preprocess")
            self._input_dir_dc = join(self.test_dir, "data_datacite")
            self._output_dir_dc_lm = join(self.test_dir, "tmp_data_datacite_lm")
            self._interval = 7

        def test_dc_preprocessing(self):
            if exists(self._output_dir_dc_lm):
                shutil.rmtree(self._output_dir_dc_lm)
            self._dc_pp = DatacitePreProcessing(self._input_dir_dc, self._output_dir_dc_lm, self._interval, testing=True)
            self._dc_pp.split_input()
            entities_w_citations = []
            all_files, targz_fd = self._dc_pp.get_all_files(self._input_dir_dc, self._dc_pp._req_type)
            for file_idx, file in enumerate(all_files, 1):
                f = open(file, encoding="utf8")
                lines = [json.loads(line) for line in f]
                ents = [d for d in lines if d.get("data")]
                lists_of_dicts = [d.get("data") for d in ents if d.get("data")]
                for l in lists_of_dicts:
                    ents_w_atts = [d.get("attributes") for d in l if d.get("attributes")]
                    lists_of_relids = [d.get("relatedIdentifiers") for d in ents_w_atts if d.get("relatedIdentifiers")]
                    for lrid in lists_of_relids:
                        for e in lrid:
                            if all(elem in e for elem in  self._dc_pp._needed_info):
                            #schema = (str(ref["relatedIdentifierType"])).lower().strip()
                            #id_man = self.get_id_manager(schema, self._id_man_dict)
                                if e.get("relationType").lower().strip() in {"cites", "iscitedby", "references", "isreferencedby"}:
                                    if e.get("relatedIdentifierType").lower().strip() == "doi":
                                        entities_w_citations.append(lrid)
                                        break
                f.close()
                n_ents_w_cit = len(entities_w_citations)

                n_out_ents = 0
                all_files_out, targz_fd = self._dc_pp.get_all_files(self._output_dir_dc_lm, self._dc_pp._req_type)
                for file_idx, file in enumerate(all_files_out, 1):
                    fo = open(file, encoding="utf8")
                    lines_out = [json.loads(line) for line in fo if line]
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

                len_out_files = len([name for name in os.listdir(self._output_dir_dc_lm) if
                                     os.path.isfile(os.path.join(self._output_dir_dc_lm, name))])

                self.assertEqual(exp_n_out_file, len_out_files)

if __name__ == '__main__':
    unittest.main()
