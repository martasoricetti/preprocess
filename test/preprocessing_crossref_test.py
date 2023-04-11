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

    def test_to_validated_id_list_API(self):
        self._cr_pp2 = CrossrefPreProcessing(self.decompr_input, self._output_dir_cr, self._interval, testing=True)
        cit_list = [
            {'issue': '4', 'key': '10.7717/peerj.4375/ref-8', 'doi-asserted-by': 'publisher', 'first-page': '919', 'DOI': '10.1016/j.joi.2016.08.002', 'article-title': 'Hybrid open access—a longitudinal study', 'volume': '10', 'author': 'Björk', 'year': '2016', 'journal-title': 'Journal of Informetrics'},
            {'issue': '2', 'key': '10.7717/peerj.4375/ref-9', 'doi-asserted-by': 'publisher', 'first-page': '131', 'DOI': '10.1002/leap.1021', 'article-title': 'The open access movement at a crossroad: are the big publishers and academic social media taking over?', 'volume': '29', 'author': 'Björk', 'year': '2016', 'journal-title': 'Learned Publishing'},
            {'key': '10.7717/peerj.4375/ref-52', 'author': 'Willinsky', 'year': '2009', 'edition': '1', 'volume-title': 'The access principle: the case for open access to research and scholarship'}]

        agents_list = [{'given': 'Silvio', 'family': 'Peroni', 'sequence': 'first', 'affiliation': [], 'ORCID': 'https://orcid.org/0000-0003-0530-4305'}, {'given': 'Alexander', 'family': 'Dutton', 'sequence': 'additional', 'affiliation': []}, {'given': 'Tanya', 'family': 'Gray', 'sequence': 'additional', 'affiliation': []}, {'given': 'David', 'family': 'Shotton', 'sequence': 'additional', 'affiliation': []}]
        expected_ag = [{'given': 'Silvio', 'family': 'Peroni', 'sequence': 'first', 'affiliation': [], 'ORCID': 'orcid:0000-0003-0530-4305'}, {'given': 'Alexander', 'family': 'Dutton', 'sequence': 'additional', 'affiliation': []}, {'given': 'Tanya', 'family': 'Gray', 'sequence': 'additional', 'affiliation': []}, {'given': 'David', 'family': 'Shotton', 'sequence': 'additional', 'affiliation': []}]

        contaier = [{'id': '2167-8359', 'schema': 'issn'}]
        expected_cont = ['issn:2167-8359']


        expected_cit = [{'issue': '4', 'key': '10.7717/peerj.4375/ref-8', 'doi-asserted-by': 'publisher', 'first-page': '919', 'article-title': 'Hybrid open access—a longitudinal study', 'volume': '10', 'author': 'Björk', 'year': '2016', 'journal-title': 'Journal of Informetrics', 'DOI': 'doi:10.1016/j.joi.2016.08.002'}, {'issue': '2', 'key': '10.7717/peerj.4375/ref-9', 'doi-asserted-by': 'publisher', 'first-page': '131', 'article-title': 'The open access movement at a crossroad: are the big publishers and academic social media taking over?', 'volume': '29', 'author': 'Björk', 'year': '2016', 'journal-title': 'Learned Publishing', 'DOI': 'doi:10.1002/leap.1021'}]

        self.assertEqual(self._cr_pp2.to_validated_id_list(cit_list, "citations"), expected_cit)
        self.assertEqual(self._cr_pp2.to_validated_id_list(agents_list, "responsible_agents"), expected_ag)
        self.assertEqual(self._cr_pp2.to_validated_id_list(contaier, "container"), expected_cont)

    def test_to_validated_id_list_DB(self):
        self._cr_pp3 = CrossrefPreProcessing(self.decompr_input, self._output_dir_cr, self._interval, testing=True)
        cit_list = [
            {'DOI': '10.7717/FAKEDOI/ref-9', 'article-title': 'The open access movement at a crossroad: are the big publishers and academic social media taking over?'}
        ]
        expected = []
        outp = self._cr_pp3.to_validated_id_list(cit_list, "citations")
        self.assertEqual(outp, expected)

        # check id management of the same id after it was inserted in the db (preferred validation option)
        if self._cr_pp3._redis_db.get("doi:10.7717/fakedoi/ref-9"):
            self._cr_pp3._redis_db.delete("doi:10.7717/fakedoi/ref-9")
        self._cr_pp3._redis_db.set("doi:10.7717/fakedoi/ref-9", "meta:8000000000")
        expected2 = [
            {'DOI': 'doi:10.7717/fakedoi/ref-9', 'article-title': 'The open access movement at a crossroad: are the big publishers and academic social media taking over?'}
        ]
        outp2 = self._cr_pp3.to_validated_id_list(cit_list, "citations")
        self.assertEqual(outp2, expected2)
        self._cr_pp3._redis_db.delete("doi:10.7717/fakedoi/ref-9")

    def test_splitted_to_file(self):
        if exists(self._output_dir_cr):
            shutil.rmtree(self._output_dir_cr)
        data_to_file = [{'publisher': 'PeerJ', 'type': 'journal-article', 'page': 'e4375', 'title': ['The state of OA: a large-scale analysis of the prevalence and impact of Open Access articles'], 'prefix': '10.7717', 'volume': '6', 'member': '4443', 'container-title': ['PeerJ'], 'original-title': [], 'deposited': {'date-parts': [[2018, 2, 13]], 'date-time': '2018-02-13T08:54:43Z', 'timestamp': 1518512083000}, 'issued': {'date-parts': [[2018, 2, 13]]}, 'reference': [{'issue': '4', 'key': '10.7717/peerj.4375/ref-8', 'doi-asserted-by': 'publisher', 'first-page': '919', 'article-title': 'Hybrid open access—a longitudinal study', 'volume': '10', 'author': 'Björk', 'year': '2016', 'journal-title': 'Journal of Informetrics', 'DOI': 'doi:10.1016/j.joi.2016.08.002'}, {'issue': '2', 'key': '10.7717/peerj.4375/ref-9', 'doi-asserted-by': 'publisher', 'first-page': '131', 'article-title': 'The open access movement at a crossroad: are the big publishers and academic social media taking over?', 'volume': '29', 'author': 'Björk', 'year': '2016', 'journal-title': 'Learned Publishing', 'DOI': 'doi:10.1002/leap.1021'}], 'DOI': '10.7717/peerj.4375', 'author': [{'given': 'Heather', 'family': 'Piwowar', 'sequence': 'first', 'affiliation': [{'name': 'Impactstory, Sanford, NC, USA'}]}, {'given': 'Jason', 'family': 'Priem', 'sequence': 'additional', 'affiliation': [{'name': 'Impactstory, Sanford, NC, USA'}]}, {'given': 'Vincent', 'family': 'Larivière', 'sequence': 'additional', 'affiliation': [{'name': 'École de bibliothéconomie et des sciences de l’information, Université de Montréal, Montréal, QC, Canada'}, {'name': 'Observatoire des Sciences et des Technologies (OST), Centre Interuniversitaire de Recherche sur la Science et la Technologie (CIRST), Université du Québec à Montréal, Montréal, QC, Canada'}]}, {'given': 'Juan Pablo', 'family': 'Alperin', 'sequence': 'additional', 'affiliation': [{'name': 'Canadian Institute for Studies in Publishing, Simon Fraser University, Vancouver, BC, Canada'}, {'name': 'Public Knowledge Project, Canada'}]}, {'given': 'Lisa', 'family': 'Matthias', 'sequence': 'additional', 'affiliation': [{'name': 'Scholarly Communications Lab, Simon Fraser University, Vancouver, Canada'}]}, {'given': 'Bree', 'family': 'Norlander', 'sequence': 'additional', 'affiliation': [{'name': 'Information School, University of Washington, Seattle, USA'}, {'name': 'FlourishOA, USA'}]}, {'given': 'Ashley', 'family': 'Farley', 'sequence': 'additional', 'affiliation': [{'name': 'Information School, University of Washington, Seattle, USA'}, {'name': 'FlourishOA, USA'}]}, {'given': 'Jevin', 'family': 'West', 'sequence': 'additional', 'affiliation': [{'name': 'Information School, University of Washington, Seattle, USA'}]}, {'given': 'Stefanie', 'family': 'Haustein', 'sequence': 'additional', 'affiliation': [{'name': 'Observatoire des Sciences et des Technologies (OST), Centre Interuniversitaire de Recherche sur la Science et la Technologie (CIRST), Université du Québec à Montréal, Montréal, QC, Canada'}, {'name': 'School of Information Studies, University of Ottawa, Ottawa, ON, Canada'}]}], 'ISSN': ['issn:2167-8359']}, {'publisher': 'Emerald', 'issue': '2', 'type': 'journal-article', 'page': '253-277', 'title': ['Setting our bibliographic references free: towards open citation data'], 'prefix': '10.1108', 'volume': '71', 'member': '140', 'container-title': ['Journal of Documentation'], 'original-title': [], 'deposited': {'date-parts': [[2019, 8, 21]], 'date-time': '2019-08-21T01:41:41Z', 'timestamp': 1566351701000}, 'issued': {'date-parts': [[2015, 3, 9]]}, 'reference': [{'key': 'b1', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1023/a:1021919228368'}, {'key': 'b5', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1042/bj20091474'}, {'key': 'b7', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1016/j.websem.2013.05.001'}, {'key': 'b9', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1523/jneurosci.0003-08.2008'}, {'key': 'b11', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1371/journal.pcbi.0010034'}, {'key': 'b14', 'unstructured': 'Cameron, R.D. (1997), “A universal citation database as a catalyst for reform in scholarly communication”,First Monday, Vol. 2 No. 4, available at: http://firstmonday.org/ojs/index.php/fm/article/view/522/443 (accessed 24 February 2014).', 'doi-asserted-by': 'crossref', 'DOI': 'doi:10.5210/fm.v2i4.522'}, {'key': 'b17', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.5539/ass.v9n5p18'}, {'key': 'b21', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1108/eum0000000007123'}, {'key': 'b22', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1136/bmj.a568'}, {'key': 'b25', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1126/science.149.3683.510'}, {'key': 'b100', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1007/s11192-009-0021-2'}, {'key': 'b31', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1001/jama.295.1.90'}, {'key': 'b32', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1073/pnas.0407743101'}, {'key': 'b33', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1136/bmj.b2680'}, {'key': 'b34', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1038/502298a'}, {'key': 'b40', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1038/35079151'}, {'key': 'b41', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1108/jd-07-2012-0082'}, {'key': 'b42', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1002/(sici)1097-4571(198909)40:5<342::aid-asi7>3.0.co;2-u'}, {'key': 'b43', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1145/1498765.1498780'}, {'key': 'b45', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1177/030631277500500106'}, {'key': 'b49', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1016/j.websem.2012.08.001'}, {'key': 'b53', 'unstructured': 'Piwowar, H. (2013), “Altmetrics: value all research products”,Nature, Vol. 493 No. 7431, pp. 159-159.', 'doi-asserted-by': 'crossref', 'DOI': 'doi:10.1038/493159a'}, {'key': 'b52', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.7717/peerj.175'}, {'key': 'b51', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1371/journal.pone.0000308'}, {'key': 'b54', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1038/495437a'}, {'key': 'b56', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1007/s10579-012-9211-2'}, {'key': 'b57', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1371/journal.pntd.0000228'}, {'key': 'b59', 'unstructured': 'Roemer, R.C. and Borchardt, R. (2012), “From bibliometrics to altmetrics: a changing scholarly landscape”,College & Research Libraries News, Vol. 73 No. 10, pp. 596-600, available at: http://crln.acrl.org/content/73/10/596.full (accessed 24 February 2014).', 'doi-asserted-by': 'crossref', 'DOI': 'doi:10.5860/crln.73.10.8846'}, {'key': 'b60', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1087/2009202'}, {'key': 'b61', 'doi-asserted-by': 'publisher', 'year': '2013', 'DOI': 'doi:10.1038/502295a'}, {'key': 'b62', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1371/journal.pcbi.1000361'}, {'key': 'b63', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1101/sqb.1972.036.01.015'}, {'key': 'b64', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1002/asi.4630240406'}, {'key': 'b65', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1177/030631277400400102'}, {'key': 'b67', 'unstructured': 'Teufel, S. , Siddharthan, A. and Tidhar, D. (2006), “Automatic classification of citation function”, Proceedings of the 2006 Conference on Empirical Methods in Natural Language Processing (EMNLP 06), Association for Computational Linguistics, Stroudsburg, PA, pp. 103-110.', 'doi-asserted-by': 'crossref', 'DOI': 'doi:10.3115/1610075.1610091'}, {'key': 'b71', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1007/bf02457980'}, {'key': 'frd1', 'doi-asserted-by': 'publisher', 'DOI': 'doi:10.1525/bio.2010.60.5.2'}], 'DOI': '10.1108/jd-12-2013-0166', 'author': [{'given': 'Silvio', 'family': 'Peroni', 'sequence': 'first', 'affiliation': [], 'ORCID': 'orcid:0000-0003-0530-4305'}, {'given': 'Alexander', 'family': 'Dutton', 'sequence': 'additional', 'affiliation': []}, {'given': 'Tanya', 'family': 'Gray', 'sequence': 'additional', 'affiliation': []}, {'given': 'David', 'family': 'Shotton', 'sequence': 'additional', 'affiliation': []}], 'ISSN': ['issn:0022-0418']}]

        self._dc_pp4 = CrossrefPreProcessing(self.decompr_input, self._output_dir_cr, 1, testing=True)
        count = 0
        for el in data_to_file:
            count += 1
            reduced_list = [el]
            self._dc_pp4.splitted_to_file(count, reduced_list, ".json")

        output_file_n = len(os.listdir(self._output_dir_cr))
        expected_n = 2
        self.assertEqual(output_file_n, expected_n)

        all_dict_in_out = []
        all_fi, op = self._dc_pp4.get_all_files(self._output_dir_cr, ".json")
        for fi in all_fi:
            f = open(fi, encoding="utf8")
            dict_f = json.load(f)
            data_f = dict_f.get("items")
            for lined in data_f:
                all_dict_in_out.append(lined)
            f.close()

        self.assertTrue(all(item in data_to_file for item in all_dict_in_out))
        self.assertTrue(all(item in all_dict_in_out for item in data_to_file))


if __name__ == '__main__':
    unittest.main()
