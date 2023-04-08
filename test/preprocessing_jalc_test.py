import json
import shutil
import unittest
import os.path
from preprocessing.jalc import JalcPreProcessing
from os.path import join, exists
import os.path

class JalcPPTest(unittest.TestCase):
    """This class aims at testing the methods of the classes OpenAirePreProcessing."""

    def setUp(self):
        self.test_dir = join("test", "preprocess")
        self._input_dir_dj = join(self.test_dir, "data_jalc")
        self._output_dir_dj = self.__get_output_directory("data_jalc_output")
        self._interval = 10

    def __get_output_directory(self, directory):
        directory = join("", "tmp", directory)
        if not os.path.exists(directory):
            os.makedirs(directory)
        return directory

    def test_to_validated_id_list_API(self):
        self.JAPP1 = JalcPreProcessing(self._input_dir_dj, self._output_dir_dj, self._interval, testing=True)
        # process_type == "venue"
        id_dict_list_1 = [{
                "journal_id": "1880-3016",
                "type": "ISSN",
                "issn_type": "print"
            },
            {
                "journal_id": "1880-3024",
                "type": "ISSN",
                "issn_type": "online"
            },
            {
                "journal_id": "jdsa",
                "type": "JID"
            }]
        id_dict_list_2 = [{
                "journal_id": "1880-3016",
                "type": "ISSN",
                "issn_type": "print"
            },
            {
                "journal_id": "1880-3024",
                "type": "ISSN",
                "issn_type": "online"
            },
            {
                "journal_id": "jdsa1623",
                "type": "JID"
            }]
        # process_type == "citation"
        id_dict_list_3 = [
            {
                "sequence": "1",
                "original_text": "1) Morris W Ed: The American Heritage Dictionary of the English Language New Collage Edition. Boston: Houghton Mifflin Company 1980: 1045L."
            },
            {
                "sequence": "2",
                "original_text": "2) 北村　聖: 医の倫理教育 ─日本の倫理教育の現状. 日医雑誌 2012; 140: 2563-2567."
            },
            {
                "sequence": "3",
                "doi": "10.7326/0003-4819-136-3-200202050-00012",
                "volume": "136",
                "first_page": "243",
                "publication_date": {
                    "publication_year": "2002"
                },
                "original_text": "3) ABIM Foundation: American Board of Internal Medicine, ACP-ASIM Foundation. American College of Physicians-American Society of Internal Medicine, European Federation of Internal Medicine: Medical professionalism in the new millennium: a physician charter. Ann Intern Med 2002; 136: 243-246."
            },
            {
                "sequence": "4",
                "original_text": "4) Medical Professionalism Project: Medical professionalism in the new millennium: a physicians' charter. Lancet 2002; 359: 520-522."
            },
            {
                "sequence": "5",
                "original_text": "5) 李　啓充: 続アメリカ医療の光と影第1回. 新ミレニアムの医師憲章 ─連載再開にあたって. 週刊医学界新聞 2002: 2480."
            }
        ]
        id_dict_list_4 = [
            {
                "sequence": "1",
                "original_text": "1) Morris W Ed: The American Heritage Dictionary of the English Language New Collage Edition. Boston: Houghton Mifflin Company 1980: 1045L."
            },
            {
                "sequence": "2",
                "original_text": "2) 北村　聖: 医の倫理教育 ─日本の倫理教育の現状. 日医雑誌 2012; 140: 2563-2567."
            },
            {
                "sequence": "3",
                "doi": "10.7326/0003-4819-136-3-200202050-000",
                "volume": "136",
                "first_page": "243",
                "publication_date": {
                    "publication_year": "2002"
                },
                "original_text": "3) ABIM Foundation: American Board of Internal Medicine, ACP-ASIM Foundation. American College of Physicians-American Society of Internal Medicine, European Federation of Internal Medicine: Medical professionalism in the new millennium: a physician charter. Ann Intern Med 2002; 136: 243-246."
            },
            {
                "sequence": "4",
                "original_text": "4) Medical Professionalism Project: Medical professionalism in the new millennium: a physicians' charter. Lancet 2002; 359: 520-522."
            },
            {
                "sequence": "5",
                "original_text": "5) 李　啓充: 続アメリカ医療の光と影第1回. 新ミレニアムの医師憲章 ─連載再開にあたって. 週刊医学界新聞 2002: 2480."
            }
        ]
        #process_type="citing_entity"
        id_dict_list_5 = "10.11231/jaem.32.907"
        id_dict_list_6 = "10.11231/jaem.32.9"

        expected1 = ["issn:1880-3016", "issn:1880-3024", "jid:jdsa"]
        expected2 = ["issn:1880-3016", "issn:1880-3024"]
        expected3 = [{"doi": "doi:10.7326/0003-4819-136-3-200202050-00012", "volume": "136", "first_page": "243", "publication_date": {"publication_year": "2002"}}]
        expected4 = []
        expected5 = "doi:10.11231/jaem.32.907"
        expected6 = None


        outp = self.JAPP1.to_validated_id_list(id_dict_list_1, "venue")
        # the JID id is not valid
        outp2 = self.JAPP1.to_validated_id_list(id_dict_list_2, "venue")

        outp3 = self.JAPP1.to_validated_id_list(id_dict_list_3, "citation")
        #the doi of the cited entity is not valid
        outp4 = self.JAPP1.to_validated_id_list(id_dict_list_4, "citation")

        outp5 = self.JAPP1.to_validated_id_list(id_dict_list_5, "citing_entity")
        # the doi of the citing entity is not valid
        outp6 = self.JAPP1.to_validated_id_list(id_dict_list_6, "citing_entity")


        self.assertEqual(outp, expected1)
        self.assertEqual(outp2, expected2)
        self.assertEqual(outp3, expected3)
        self.assertEqual(outp4, expected4)
        self.assertEqual(outp5, expected5)
        self.assertEqual(outp6, expected6)

    def test_to_validated_id_list_DB(self):
        self.JAPP2 = JalcPreProcessing(self._input_dir_dj, self._output_dir_dj, self._interval, testing=True)
        # check id management of an id which does not exist - API
        doi_invalid = "10.11231/jaem.32.90"
        expected = None
        outp = self.JAPP2.to_validated_id_list(doi_invalid, "citing_entity")
        self.assertEqual(outp, expected)
        #check id management of a non valid-id after it was inserted in the db (preferred validation option)
        self.JAPP2._redis_db.set("doi:10.11231/jaem.32.90", "0000000000")
        expected = "doi:10.11231/jaem.32.90"
        outp = self.JAPP2.to_validated_id_list(doi_invalid, "citing_entity")
        self.assertEqual(outp, expected)

    def test_split_input(self):
        if exists(self._output_dir_dj):
            shutil.rmtree(self._output_dir_dj)
        self.assertFalse(exists(self._output_dir_dj))

        self.JAPP = JalcPreProcessing(self._input_dir_dj, self._output_dir_dj, self._interval, testing=True)
        self.JAPP.split_input()

        entities_w_citations = []
        # iterate over the input data
        all_files = self.JAPP.get_all_files(self._input_dir_dj, ".zip")
        for i, zipped_folder in enumerate(all_files, 1):
            if zipped_folder:
                for n, el in enumerate(zipped_folder):
                    if el:
                        all_files_unzipped = self.JAPP.get_all_files(el, self.JAPP._req_type)
                        for folder_idx, folder in enumerate(all_files_unzipped, 1):
                            if folder:
                                for file_idx, file in enumerate(folder, 1):
                                    f = open(file, encoding="utf-8")
                                    my_dict = json.load(f)
                                    d = my_dict.get("data")
                                    # filtering out entities without citations
                                    if "citation_list" in d:
                                        cit_list = d["citation_list"]
                                        cit_list_doi = [x for x in cit_list if x.get("doi")]
                                        # filtering out entities with citations without dois
                                        if cit_list_doi:
                                            # citing_entity
                                            citing_id_to_keep = self.JAPP.to_validated_id_list(d.get("doi"), "citing_entity")
                                            if citing_id_to_keep:
                                                citations = d.get("citation_list")
                                                processed_citations = self.JAPP.to_validated_id_list(citations, "citation")
                                                if processed_citations:
                                                    entities_w_citations.append(citing_id_to_keep)
                                    f.close()
        n_ents_w_cit = len(entities_w_citations)

        n_out_ents = 0
        all_files_out, targz_fd = self.JAPP.get_all_files(self._output_dir_dj, ".ndjson")
        for file_idx, file in enumerate(all_files_out, 1):
            fo = open(file, encoding="utf8")
            lines_out = [line for line in fo if line]
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

        len_out_files = len([name for name in os.listdir(self._output_dir_dj) if
                             os.path.isfile(os.path.join(self._output_dir_dj, name))])

        self.assertEqual(exp_n_out_file, len_out_files)

        # for f in os.listdir(self._input_dir_dj):
        #     decompr_dir_filepath = join(self._input_dir_dj, f.split(".")[0] + "_decompr_zip_dir")
        #
        #     if exists(decompr_dir_filepath):
        #         shutil.rmtree(decompr_dir_filepath)


 #python -m unittest discover -s test -p "preprocessing_jalc_test.py"
if __name__ == '__main__':
    unittest.main()


