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


class PreprocessingTestDatacite(unittest.TestCase):
        def setUp(self):
            self.test_dir = join("test", "preprocess")
            self._input_dir_dc = join(self.test_dir, "data_datacite", "ndjson_files")
            self._compr_input_dir_dc = join(self.test_dir, "data_datacite", "sample_9.ndjson.zst")
            self._output_dir_dc_lm = self.__get_output_directory("data_datacite_output")
            self._compr_output_dir_dc_lm = self.__get_output_directory("data_datacite_output_compress")
            self._interval = 7

        def __get_output_directory(self, directory):
            directory = join("", "tmp", directory)
            if not os.path.exists(directory):
                os.makedirs(directory)
            return directory

        def test_split_input(self):
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

        def test_split_input_compress(self):
            if exists(self._compr_output_dir_dc_lm):
                shutil.rmtree(self._compr_output_dir_dc_lm)
            self._dc_pp = DatacitePreProcessing(self._compr_input_dir_dc, self._compr_output_dir_dc_lm, self._interval, testing=True)
            self._dc_pp.split_input()
            entities_w_citations = []
            all_files, targz_fd = self._dc_pp.get_all_files(self._compr_input_dir_dc, self._dc_pp._req_type)
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
            all_files_out, targz_fd = self._dc_pp.get_all_files(self._compr_output_dir_dc_lm, self._dc_pp._req_type)
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

            len_out_files = len([name for name in os.listdir(self._compr_output_dir_dc_lm) if
                                 os.path.isfile(os.path.join(self._compr_output_dir_dc_lm, name))])

            self.assertEqual(exp_n_out_file, len_out_files)

            #remove the directory where the zst was extracted
            decompr_dir_filepath = self._compr_input_dir_dc.split(".")[0]+"_decompr_zst_dir"
            if exists(decompr_dir_filepath):
                shutil.rmtree(decompr_dir_filepath)

        def test_to_validated_id_list_API(self):
            self._dc_pp2 = DatacitePreProcessing(self._input_dir_dc, self._output_dir_dc_lm, self._interval, testing=True)

            identifiers = [{'type': 'Series', 'identifier': '10.11646/zootaxa.3644.1.1', 'identifierType': 'DOI'}, {'type': 'Journal Article', 'identifier': '1248', 'identifierType': 'PMID'}]
            container_w_id = [{'type': 'Series', 'identifier': '10.11646/zootaxa.3644.1.1', 'identifierType': 'DOI'}]
            container_no_id = [{'type': 'Book', 'title': 'A Book Title', 'firstPage': "1", 'volume':"IV", 'issue':'6', 'lastPage':'14'}]
            container_wrong_id = [{'type': 'Book', 'title': 'A Book Title', 'firstPage': "1", 'volume':"IV", 'issue':'6', 'lastPage':'14', 'identifier': '2151-6065', 'identifierType': 'ISSN'}]
            rel_ids_not_relevant_a = [{'relationType': 'References', 'relatedIdentifier': 'http://www.agu.org/cgi-bin/agubooks?topic=..AR&book=OSAR0759108', 'relatedIdentifierType': 'URL'}, {'relationType': 'IsVariantFormOf', 'relatedIdentifier': 'http://hs.pangaea.de/bathy/panmap/BCWS.lay.zip', 'relatedIdentifierType': 'URL'}]
            rel_ids_not_relevant_b = [{'relationType': 'IsSupplementTo', 'relatedIdentifier': '10.1016/0016-7037(96)00042-7', 'relatedIdentifierType': 'DOI'}]
            rel_ids_relevant = [{'relationType': 'IsCitedBy', 'relatedIdentifier': '10.5281/zenodo.6100281', 'resourceTypeGeneral': 'Text', 'relatedIdentifierType': 'DOI'}, {'relationType': 'References', 'relatedIdentifier': '10.1175/jpo2940.1', 'relatedIdentifierType': 'DOI'}, {'relationType': 'IsPartOf', 'relatedIdentifier': '10.11646/zootaxa.4059.3.1', 'resourceTypeGeneral': 'JournalArticle', 'relatedIdentifierType': 'DOI'}, {'relationType': 'IsPartOf', 'relatedIdentifier': '2151-6065', 'relatedIdentifierType': 'ISSN'}]
            rel_ids_mix = [
                {'relationType': 'IsPartOf',
                 'relatedIdentifier': '10.11646/zootaxa.4059.3.1',
                 'resourceTypeGeneral': 'JournalArticle',
                 'relatedIdentifierType': 'DOI'},
                {'relationType': 'IsPartOf',
                 'relatedIdentifier': 'urn:lsid:plazi.org:pub:D658250BFFA02A0DFF89763BFFB33B53',
                 'resourceTypeGeneral': 'JournalArticle',
                 'relatedIdentifierType': 'LSID'},
                {'relationType': 'IsPartOf',
                 'relatedIdentifier': 'http://publication.plazi.org/id/D658250BFFA02A0DFF89763BFFB33B53',
                 'resourceTypeGeneral': 'JournalArticle',
                 'relatedIdentifierType': 'URL'},
                {'relationType': 'IsCitedBy',
                 'relatedIdentifier': '10.5281/zenodo.6100281',
                 'resourceTypeGeneral': 'Text',
                 'relatedIdentifierType': 'DOI'},
                {'relationType': 'IsCitedBy',
                 'relatedIdentifier': 'http://treatment.plazi.org/id/2A615D73FF822A2FFF1E743BFEA03C18',
                 'resourceTypeGeneral': 'Text', 'relatedIdentifierType': 'URL'}]

            creators_w_ids = [
                {'name': 'Peroni, Silvio',
                 'nameType': 'Personal',
                 'givenName': 'Silvio',
                 'familyName': 'Peroni',
                 'affiliation': [],
                 'nameIdentifiers':
                     [
                         {
                             'nameIdentifierScheme': 'ORCID',
                             'nameIdentifier': '0000-0003-0530-4305'
                         },
                         {
                             'nameIdentifierScheme': 'VIAF',
                             'nameIdentifier': '309649450'
                         }
                     ]
                 },
                {
                    'name': 'Heibi, Ivan',
                    'affiliation': [],
                    'nameType': 'Personal',
                    'nameIdentifiers': [
                        {
                            'nameIdentifierScheme': 'ORCID',
                            'nameIdentifier': '0000-0001-5366-5194'
                        }
                    ]
                }
            ]
            creators_no_ids = [
                {'name': 'Ovchinnikov, I Michailovich', 'nameType': 'Personal', 'givenName': 'I Michailovich', 'familyName': 'Ovchinnikov', 'affiliation': [], 'nameIdentifiers': []}, {'name': 'GOIN', 'affiliation': [], 'nameIdentifiers': []}]

            outp = self._dc_pp2.to_validated_id_list(identifiers, "identifiers")
            expected = ['pmid:1248']

            outp2 = self._dc_pp2.to_validated_id_list(container_w_id, "container")
            expected_2 = {'type': 'Series', 'identifier': ['doi:10.11646/zootaxa.3644.1.1']}

            outp2b = self._dc_pp2.to_validated_id_list(container_no_id, "container")
            expected_2b = {'type': 'Book', 'title': 'A Book Title', 'firstPage': '1', 'volume': 'IV', 'issue': '6', 'lastPage': '14', 'identifier': []}

            outp2c = self._dc_pp2.to_validated_id_list(container_wrong_id, "container")
            expected_2c = {'type': 'Book', 'title': 'A Book Title', 'firstPage': '1', 'volume': 'IV', 'issue': '6', 'lastPage': '14', 'identifier': []}


            outp3 = self._dc_pp2.to_validated_id_list(rel_ids_not_relevant_a, "related_ids")
            outp3a = self._dc_pp2.to_validated_id_list(rel_ids_not_relevant_b, "related_ids")
            expected_3 = [], [], []
            expected_3a = [], [], []

            outp3b= self._dc_pp2.to_validated_id_list(rel_ids_relevant, "related_ids")
            expected_3b = ['doi:10.1175/jpo2940.1'], ['doi:10.5281/zenodo.6100281'], ['doi:10.11646/zootaxa.4059.3.1', 'issn:2151-6065']

            outp4= self._dc_pp2.to_validated_id_list(rel_ids_mix, "related_ids")
            expected_4 = [], ['doi:10.5281/zenodo.6100281'], ['doi:10.11646/zootaxa.4059.3.1']

            outp6 = self._dc_pp2.to_validated_id_list(creators_w_ids, "creators")
            expected_6 = [{'name': 'Peroni, Silvio', 'nameType': 'Personal', 'givenName': 'Silvio', 'familyName': 'Peroni', 'nameIdentifiers': ['orcid:0000-0003-0530-4305', 'viaf:309649450']}, {'name': 'Heibi, Ivan', 'nameType': 'Personal', 'nameIdentifiers': ['orcid:0000-0001-5366-5194']}]

            outp7 = self._dc_pp2.to_validated_id_list(creators_no_ids, "creators")
            expected_7 = [{'name': 'Ovchinnikov, I Michailovich', 'nameType': 'Personal', 'givenName': 'I Michailovich', 'familyName': 'Ovchinnikov', 'nameIdentifiers': []}, {'name': 'GOIN', 'nameIdentifiers': []}]


            self.assertEqual(outp2, expected_2)
            self.assertEqual(outp, expected)
            self.assertEqual(outp2b, expected_2b)
            self.assertEqual(outp2c, expected_2c)
            self.assertEqual(outp3, expected_3)
            self.assertEqual(outp3b, expected_3b)
            self.assertEqual(outp3a, expected_3a)
            self.assertEqual(outp4, expected_4)
            self.assertEqual(outp6, expected_6)
            self.assertEqual(outp7, expected_7)

        def test_to_validated_id_list_DB(self):
            self._dc_pp3 = DatacitePreProcessing(self._input_dir_dc, self._output_dir_dc_lm, self._interval, testing=True)
            id_dict_list = [{'relationType': 'IsCitedBy', 'relatedIdentifier': '9999999999999', 'resourceTypeGeneral': 'Text', 'relatedIdentifierType': 'PMID'}]
            expected = [], [], []
            # check id management of an id which does not exist - API
            outp = self._dc_pp3.to_validated_id_list(id_dict_list, "related_ids")
            self.assertEqual(outp, expected)

            # check id management of the same id after it was inserted in the db (preferred validation option)
            if self._dc_pp3._redis_db.get("pmid:9999999999999"):
                self._dc_pp3._redis_db.delete("pmid:9999999999999")
            self._dc_pp3._redis_db.set("pmid:9999999999999", "meta:0000000000")
            expected = [], ["pmid:9999999999999"], []
            outp = self._dc_pp3.to_validated_id_list(id_dict_list, "related_ids")
            self.assertEqual(outp, expected)
            self._dc_pp3._redis_db.delete("pmid:9999999999999")

        def test_splitted_to_file(self):
            if exists(self._output_dir_dc_lm):
                shutil.rmtree(self._output_dir_dc_lm)
            data_to_file = [
                {'titles': [{'title': 'Gonatocerus (Gonatocerus) pictus Haliday 1833'}], 'publisher': 'Zenodo',
              'publicationYear': 2013, 'dates': [{'date': '2013-04-30', 'dateType': 'Issued'}],
              'types': {'ris': 'RPRT', 'bibtex': 'article', 'citeproc': 'article-journal',
                        'schemaOrg': 'ScholarlyArticle', 'resourceType': 'Taxonomic treatment',
                        'resourceTypeGeneral': 'Text'}, 'updated': '2022-08-03T14:59:12Z',
              'doi': '10.5281/zenodo.5099028', 'relatedIdentifiers': {
                    'Cites': ['doi:10.5281/zenodo.246902', 'doi:10.5281/zenodo.246903', 'doi:10.5281/zenodo.246904'],
                    'IsCitedBy': [], 'IsPartOf': ['doi:10.11646/zootaxa.3644.1.1']}, 'contributors': [], 'creators': [
                    {'name': 'Triapitsyn, Serguei V.', 'givenName': 'Serguei V.', 'familyName': 'Triapitsyn',
                     'nameIdentifiers': []}], 'identifiers': [],
              'container': {'type': 'Series', 'identifier': ['doi:10.11646/zootaxa.3644.1.1']}},
                {'titles': [{'title': 'Physical oceanography of the GOIN project in the Mediterranean Sea (1987-1990)'}],
              'publisher': 'PANGAEA - Data Publisher for Earth & Environmental Science', 'publicationYear': 2008,
              'dates': [{'date': '2008', 'dateType': 'Issued'}],
              'types': {'ris': 'GEN', 'bibtex': 'misc', 'citeproc': 'article', 'schemaOrg': 'Collection',
                        'resourceType': 'Collection of Datasets', 'resourceTypeGeneral': 'Collection'},
              'updated': '2020-07-28T04:25:36.000Z', 'doi': '10.1594/pangaea.710738',
              'relatedIdentifiers': {'Cites': ['doi:10.1175/jpo2940.1'], 'IsCitedBy': [], 'IsPartOf': []},
              'contributors': [
                  {'name': 'State Oceanographic Institute, Moscow', 'nameType': 'Personal', 'givenName': 'Moscow',
                   'familyName': 'State Oceanographic Institute', 'contributorType': 'HostingInstitution',
                   'nameIdentifiers': []}], 'creators': [
                 {'name': 'Ovchinnikov, I Michailovich', 'nameType': 'Personal', 'givenName': 'I Michailovich',
                  'familyName': 'Ovchinnikov', 'nameIdentifiers': []}, {'name': 'GOIN', 'nameIdentifiers': []}],
              'identifiers': [], 'container': {}}
            ]

            self._dc_pp4 = DatacitePreProcessing(self._input_dir_dc, self._output_dir_dc_lm, 1, testing=True)
            count = 0
            for el in data_to_file:
                count += 1
                reduced_list = [el]
                self._dc_pp4.splitted_to_file(count, reduced_list, ".ndjson")

            output_file_n = len(os.listdir(self._output_dir_dc_lm))
            expected_n = 2
            self.assertEqual(output_file_n, expected_n)

            all_dict_in_out = []
            all_fi, op = self._dc_pp4.get_all_files(self._output_dir_dc_lm, ".ndjson")
            for fi in all_fi:
                f = open(fi, encoding="utf8")
                for line in f:
                    lined = json.loads(line)
                    all_dict_in_out.append(lined)
                f.close()

            self.assertTrue(all(item in data_to_file for item in all_dict_in_out))
            self.assertTrue(all(item in all_dict_in_out for item in data_to_file))


if __name__ == '__main__':
    unittest.main()
