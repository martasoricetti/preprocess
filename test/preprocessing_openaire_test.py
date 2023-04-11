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
from preprocessing.openaire import OpenirePreProcessing
import shutil
import os
import gzip
import json


class OpenAirePPTest(unittest.TestCase):
    """This class aims at testing the methods of the classes OpenAirePreProcessing."""

    def setUp(self):
        self.test_dir = join("test", "preprocess")
        self.req_type = ".gz"
        self.num_0 = 5
        self.num_1 = 8

        # OpenArie data, for OROCI parser
        self.input_dir = join(self.test_dir, "data_openaire")
        self.output_dir = self.__get_output_directory("data_openaire_output")


    def __get_output_directory(self, directory):
        directory = join("", "tmp", directory)
        if not os.path.exists(directory):
            os.makedirs(directory)
        return directory

    def test_to_validated_id_list_API(self):
        self.OAPP2 = OpenirePreProcessing(self.input_dir, self.output_dir, self.num_1, testing=True)
        id_dict_list = [{"identifier":"PMID:1284", "schema":"pmid"}, {"identifier":"DOI:10.1016/0531-5565(75)90003-0","schema":"doi"}]
        id_dict_list_2 = [{"identifier":"1284", "schema":"PMID"}, {"identifier":"10.1016/0531-5565(75)90003-0","schema":"DOI"}]
        id_dict_list_3 = [{"identifier":"1284", "schema":"PMID"}, {"identifier":"10.1016/0531-5565(75)90003-0","schema":"DOI"}, {"identifier":"https://ror.org/02mhbdp94","schema":"ror"}]
        expected = ['pmid:1284', 'doi:10.1016/0531-5565(75)90003-0']
        #check id normalization management
        outp = self.OAPP2.to_validated_id_list(id_dict_list)
        #check id management without prefix, with schema in uppercase
        outp2 = self.OAPP2.to_validated_id_list(id_dict_list_2)
        #check id exclusion in case of not handled schema
        outp3 = self.OAPP2.to_validated_id_list(id_dict_list_3)
        self.assertEqual(outp, expected)
        self.assertEqual(outp2, expected)
        self.assertEqual(outp3, expected)

    def test_to_validated_id_list_DB(self):
        self.OAPP3 = OpenirePreProcessing(self.input_dir, self.output_dir, self.num_1, testing=True)
        id_dict_list = [{"identifier":"pmid:89999999999999", "schema":"pmid"}]
        expected = []
        #check id management of an id which does not exist - API
        outp = self.OAPP3.to_validated_id_list(id_dict_list)
        self.assertEqual(outp, expected)

        #check id management of the same id after it was inserted in the db (preferred validation option)
        if self.OAPP3._redis_db.get("pmid:89999999999999"):
            self.OAPP3._redis_db.delete("pmid:89999999999999")
        self.OAPP3._redis_db.set("pmid:89999999999999", "0000000000")
        expected = ["pmid:89999999999999"]
        outp = self.OAPP3.to_validated_id_list(id_dict_list)
        self.assertEqual(outp, expected)
        self.OAPP3._redis_db.delete("pmid:89999999999999")


    def test_split_input(self):
        if exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        self.assertFalse(exists(self.output_dir))

        self.OAPP = OpenirePreProcessing(self.input_dir, self.output_dir, self.num_1, testing=True)
        self.OAPP.split_input()

        #delete all decompressed subdirectories just generated with get_all_files, in order to avoid counting data twice
        for root, dirs, files in os.walk(self.input_dir):
            for d in dirs:
                if not d.endswith(".tar"):
                    shutil.rmtree(join(root,d))

        # checks that the output directory is generated in the process.
        self.assertTrue(exists(self.output_dir))
        x = 0
        len_total_ent = 0
        data = []
        for tar in os.listdir(self.input_dir):
            all_files, targz_fd = self.OAPP.get_all_files(os.path.join(self.input_dir,tar), self.req_type)
            for file_idx, file in enumerate(all_files, 1):
                f = gzip.open(file, 'rb')
                file_content = f.readlines()  # list
                for entity in file_content:
                    if entity:
                        x += 1
                        decoded = json.loads(entity.decode('utf-8'))
                        type = (decoded.get("relationship")).get("name")
                        if type == "Cites":
                            citante_data = decoded.get("source")
                            citante_ids = citante_data.get("identifier")
                            accepted_ids_citing = [x["identifier"] for x in citante_ids if x["schema"] in self.OAPP._accepted_ids]
                            if accepted_ids_citing:
                                citato_data = decoded.get("target")
                                citato_ids = citato_data.get("identifier")
                                accepted_ids_cited = [x["identifier"] for x in citato_ids if x["schema"] in self.OAPP._accepted_ids]
                                if accepted_ids_cited:
                                    data.append(decoded)
                                    len_total_ent += 1

        expected_num_files = len_total_ent // self.num_1 if len_total_ent % self.num_1 == 0 else len_total_ent // self.num_1 + 1
        files, targz_fd = self.OAPP.get_all_files(self.output_dir, self.req_type)
        len_files = len(files)

        self.assertEqual(len_files, expected_num_files)

        len_lines_output = 0
        for idx, file in enumerate(files):
            fo = gzip.open(file, 'rb')
            file_content_o = fo.readlines()  # list

            for entity in file_content_o:
                if entity:
                    len_lines_output += 1


        self.assertEqual(len_total_ent, len_lines_output)

        #delete all decompressed subdirectories just generated with get_all_files, in order to avoid counting data twice
        for root, dirs, files in os.walk(self.input_dir):
            for d in dirs:
                if not d.endswith(".tar"):
                    shutil.rmtree(join(root,d))

    #def test_redis_server(self):
        #self.OAPP4 = OpenirePreProcessing(self.input_dir, self.output_dir, self.num_1)
        #self.OAPP4.split_input()
        #for root, dirs, files in os.walk(self.input_dir):
            #for d in dirs:
                #if not d.endswith(".tar"):
                    #shutil.rmtree(join(root,d))


if __name__ == '__main__':
    unittest.main()
