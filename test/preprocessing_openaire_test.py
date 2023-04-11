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
        self.num_1 = 4

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
        id_dict_list_4 = [{"identifier":"2151-6065", "schema":"ISSN"}, {"identifier":"10.1016/INVALIDDOI","schema":"DOI"}, {"identifier":"https://ror.org/02mhbdp94","schema":"ror"}] # ISSN: schema not accepted, DOI 10.1016/INVALIDDOI : schema accepted but invalid id, ROR: schema not accepted

        expected = ['pmid:1284', 'doi:10.1016/0531-5565(75)90003-0']
        expected_inv = []

        #check id normalization management
        outp = self.OAPP2.to_validated_id_list(id_dict_list, "citations")
        #check id management without prefix, with schema in uppercase
        outp2 = self.OAPP2.to_validated_id_list(id_dict_list_2, "citations")
        #check id exclusion in case of not handled schema
        outp3 = self.OAPP2.to_validated_id_list(id_dict_list_3, "citations")
        outp4 = self.OAPP2.to_validated_id_list(id_dict_list_4, "citations")
        self.assertEqual(outp, expected)
        self.assertEqual(outp2, expected)
        self.assertEqual(outp3, expected)
        self.assertEqual(outp4, expected_inv) # ISSN: schema not accepted, DOI 10.1016/INVALIDDOI : schema accepted but invalid id, ROR: schema not accepted

    def test_to_validated_id_list_DB(self):
        self.OAPP3 = OpenirePreProcessing(self.input_dir, self.output_dir, self.num_1, testing=True)
        id_dict_list = [{"identifier":"pmid:89999999999999", "schema":"pmid"}]
        expected = []
        #check id management of an id which does not exist - API
        outp = self.OAPP3.to_validated_id_list(id_dict_list, "citations")
        self.assertEqual(outp, expected)

        #check id management of the same id after it was inserted in the db (preferred validation option)
        if self.OAPP3._redis_db.get("pmid:89999999999999"):
            self.OAPP3._redis_db.delete("pmid:89999999999999")
        self.OAPP3._redis_db.set("pmid:89999999999999", "0000000000")
        expected = ["pmid:89999999999999"]
        outp = self.OAPP3.to_validated_id_list(id_dict_list, "citations")
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

    def test_splitted_to_file(self):
        if exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        data_to_file = [
            {'relationship': {'name': 'Cites', 'schema': 'datacite', 'inverse': 'IsCitedBy'}, 'source': {'objectType': 'publication', 'objectSubType': 'Article', 'title': 'Profiling, Bioinformatic, and Functional Data on the Developing Olfactory/GnRH System Reveal Cellular and Molecular Pathways Essential for This Process and Potentially Relevant for the Kallmann Syndrome', 'publicationDate': '2013-12-01', 'identifier': ['pmcid:PMC3876029', 'pmid:24427155'], 'publisher': [{'name': 'Frontiers Media S.A.', 'identifiers': None}], 'creator': [{'name': 'Garaffo, Giulia', 'identifiers': None}, {'name': 'Provero, Paolo', 'identifiers': None}, {'name': 'Molineris, Ivan', 'identifiers': None}, {'name': 'Pinciroli, Patrizia', 'identifiers': None}, {'name': 'Peano, Clelia', 'identifiers': None}, {'name': 'Battaglia, Cristina', 'identifiers': None}, {'name': 'Tomaiuolo, Daniela', 'identifiers': None}, {'name': 'Etzion, Talya', 'identifiers': None}, {'name': 'Gothilf, Yoav', 'identifiers': None}, {'name': 'Santoro, Massimo', 'identifiers': None}, {'name': 'Merlo, Giorgio R.', 'identifiers': None}]}, 'target': {'objectType': 'publication', 'objectSubType': 'Article', 'title': 'Next Generation Sequencing and Rare Genetic Variants', 'publicationDate': '2013-08-01', 'identifier': ['doi:10.1002/em.21799'], 'publisher': [{'name': 'Wiley', 'identifiers': None}], 'creator': [{'name': 'Cornelia Di Gaetano', 'identifiers': None}, {'name': 'Giuseppe Matullo', 'identifiers': None}, {'name': 'Simonetta Guarrera', 'identifiers': None}]}},
            {'relationship': {'name': 'Cites', 'schema': 'datacite', 'inverse': 'IsCitedBy'}, 'source': {'objectType': 'publication', 'objectSubType': 'Article', 'title': 'Complement enhances in vitro neutralizing potency of antibodies to human cytomegalovirus glycoprotein B (gB) and immune sera induced by gB/MF59 vaccination', 'publicationDate': '2017-12-01', 'identifier': ['pmcid:PMC5730571', 'pmid:29263890'], 'publisher': [{'name': 'Nature Publishing Group UK', 'identifiers': None}], 'creator': [{'name': 'Li, Fengsheng', 'identifiers': None}, {'name': 'Freed, Daniel C.', 'identifiers': None}, {'name': 'Tang, Aimin', 'identifiers': None}, {'name': 'Rustandi, Richard R.', 'identifiers': None}, {'name': 'Troutman, Matthew C.', 'identifiers': None}, {'name': 'Espeseth, Amy S.', 'identifiers': None}, {'name': 'Zhang, Ningyan', 'identifiers': None}, {'name': 'An, Zhiqiang', 'identifiers': None}, {'name': 'McVoy, Michael', 'identifiers': None}, {'name': 'Zhu, Hua', 'identifiers': None}, {'name': 'Ha, Sha', 'identifiers': None}, {'name': 'Wang, Dai', 'identifiers': None}, {'name': 'Adler, Stuart P.', 'identifiers': None}, {'name': 'Fu, Tong-Ming', 'identifiers': None}]}, 'target': {'objectType': 'publication', 'objectSubType': 'Other literature type', 'title': 'The complement system: its importance in the host response to viral infection.', 'publicationDate': '1982-03-01', 'identifier': ['pmcid:PMC373211', 'pmid:7045625'], 'publisher': [], 'creator': [{'name': 'Hirsch, R L', 'identifiers': None}]}}]

        self.OAPP4 = OpenirePreProcessing(self.input_dir, self.output_dir, 1, testing=True)
        count = 0
        for el in data_to_file:
            count += 1
            reduced_list = [el]
            self.OAPP4.splitted_to_file(count, reduced_list, ".gz")

        output_file_n = len(os.listdir(self.output_dir))
        expected_n = 2
        self.assertEqual(output_file_n, expected_n)

        all_dict_in_out = []
        all_fi, op = self.OAPP4.get_all_files(self.output_dir, ".gz")
        for file in all_fi:
            f = gzip.open(file, 'rb')
            file_content = f.readlines()  # list

            for entity in file_content:
                if entity:
                    d = json.loads(entity.decode('utf-8'))
                    all_dict_in_out.append(d)
            f.close()

        self.assertTrue(all(item in data_to_file for item in all_dict_in_out))
        self.assertTrue(all(item in all_dict_in_out for item in data_to_file))


if __name__ == '__main__':
    unittest.main()
