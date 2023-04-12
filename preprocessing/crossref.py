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

from argparse import ArgumentParser
from os import makedirs
import json
import os
from tqdm import tqdm
from datetime import datetime
import os.path
from os.path import exists
from preprocessing.base import Preprocessing
from oc_idmanager.doi import DOIManager
from oc_idmanager.issn import ISSNManager
from oc_idmanager.isbn import ISBNManager
from oc_idmanager.ror import RORManager
from oc_idmanager.viaf import ViafManager
from oc_idmanager.orcid import ORCIDManager


class CrossrefPreProcessing(Preprocessing):
    _req_type = ".json"
    _accepted_ids = {"doi", "issn", "isbn", "orcid"}
    _entity_keys_to_update = {"ISSN", "author", "reference", "editor", "ISBN", "DOI"}
    _entity_keys_to_keep = {"container-title", "issued", "member", "issued", "issue", "prefix", "title", "type", "publisher", "volume", "deposited", "page", "original-title", "content-updated"}

    def __init__(self, input_dir, output_dir, interval, testing=False):
        if testing:
            self._redis_db = self.BR_redis_test
            self._redis_db_ra = self.RA_redis_test
        else:
            self._redis_db = self.BR_redis
            self._redis_db_ra = self.RA_redis
        self._input_dir = input_dir
        self._output_dir = output_dir
        if not exists(self._output_dir):
            makedirs(self._output_dir)
        self._interval = interval
        self._doi_manager = DOIManager()
        self._issn_manager = ISSNManager()
        self._isbn_manager = ISBNManager()
        self._viaf_manager = ViafManager()
        self._ror_manager = RORManager()
        self._orcid_manager = ORCIDManager()
        self._id_man_dict = {"doi":self._doi_manager, "issn": self._issn_manager, "isbn": self._isbn_manager, "viaf":self._viaf_manager, "ror": self._ror_manager, "orcid":self._orcid_manager}

        super(CrossrefPreProcessing, self).__init__()

    def split_input(self):
        data = []
        count = 0

        all_files, targz_fd = self.get_all_files(self._input_dir, self._req_type)
        for file_idx, file in enumerate(tqdm(all_files), 1):
            file_data = self.load_json(file, targz_fd)
            if "items" in file_data:
                for obj in tqdm(file_data["items"]):

                    if obj.get("DOI") and obj.get("reference"):
                        doi_entity = self._doi_manager.normalise(obj['DOI'])
                        if doi_entity:

                            # add k,v pairs which do not need to be modified
                            processed_entity = {k: v for k, v in obj.items() if
                                                k in self._entity_keys_to_keep}

                            # update k,v pairs which need to be updated before being added to the processed_entity dict

                            # "reference"
                            references = self.to_validated_id_list(obj.get("reference"), "citations")
                            if references:
                                processed_entity['reference'] = references
                            else:
                                continue

                            #"DOI"
                            processed_entity['DOI'] = doi_entity

                            # "author"
                            if obj.get("author"):
                                authors = self.to_validated_id_list(obj.get("author"), "responsible_agents")
                                processed_entity['author'] = authors

                            #"editor"
                            if obj.get("editor"):
                                editors = self.to_validated_id_list(obj.get("editor"), "responsible_agents")
                                processed_entity['editor'] = editors

                            # ISSN
                            if obj.get("ISSN"):
                                get_ISSN = obj.get("ISSN")
                                if not isinstance(get_ISSN, list):
                                    get_ISSN = [get_ISSN]

                                ISSN = [{"id":x, "schema": "issn"} for x in get_ISSN]
                                ISSN_v = self.to_validated_id_list(ISSN, "container")
                                processed_entity['ISSN'] = ISSN_v

                            # ISBN
                            if obj.get("ISBN"):
                                get_ISBN = obj.get("ISBN")
                                if not isinstance(get_ISBN, list):
                                    get_ISBN = [get_ISBN]
                                ISBN = [{"id":x, "schema": "isbn"} for x in get_ISBN]
                                ISBN_v = self.to_validated_id_list(ISBN, "container")
                                processed_entity['ISBN'] = ISBN_v


                            data.append(processed_entity)
                            count += 1
                            if int(count) != 0 and int(count) % int(self._interval) == 0:
                                data = self.splitted_to_file(count, data, ".json")


        if targz_fd is not None:
            targz_fd.close()
        if len(data) > 0:
            count = count + (self._interval - (int(count) % int(self._interval)))
            self.splitted_to_file(count, data, ".json")


    def splitted_to_file(self, cur_n, data, type):
        if type == ".json":
            dict_to_json = dict()
            if int(cur_n) != 0 and int(cur_n) % int(self._interval) == 0: # and len(data)
                filename = "jSonFile_" + str(cur_n // self._interval) + self._req_type
                if exists(os.path.join(self._output_dir, filename)):
                    cur_datetime = datetime.now()
                    dt_string = cur_datetime.strftime("%d%m%Y_%H%M%S")
                    filename = filename[:-len(self._req_type)] + "_" + dt_string + self._req_type
                with open(os.path.join(self._output_dir, filename), "w", encoding="utf8") as json_file:
                    dict_to_json["items"] = data
                    json.dump(dict_to_json, json_file)
                    json_file.close()
                return []
            else:
                return data
        else:
            return data

    def to_validated_id_list(self, id_dict_list, process_type):
        if process_type == "responsible_agents":
            processed_list = []
            for c in id_dict_list:
                a_processed = {k: v for k, v in c.items() if k != "ORCID"}
                if c.get("ORCID"):
                    if isinstance(c['ORCID'], list):
                        orcid = str(c['ORCID'][0])
                    else:
                        orcid = str(c['ORCID'])
                    id_man = self.get_id_manager("orcid", self._id_man_dict)
                    norm_id = id_man.normalise(orcid, include_prefix=True)
                    if self._redis_db_ra.get(norm_id):
                        a_processed["ORCID"] = norm_id
                    elif id_man.is_valid(norm_id):
                        a_processed["ORCID"] = norm_id
                    else:
                        pass

                processed_list.append(a_processed)

            return processed_list

        elif process_type == "citations":
            id_man = self.get_id_manager("doi", self._id_man_dict)
            processed_list = []
            min_req_dict_list = [x for x in id_dict_list if x.get("DOI")]
            if min_req_dict_list:
                for c in min_req_dict_list:
                    c_processed = {k:v for k,v in c.items() if k!= "DOI"}
                    id = c.get("DOI")
                    norm_id = id_man.normalise(id, include_prefix=True)
                    if self._redis_db.get(norm_id):
                        c_processed["DOI"]= norm_id
                        processed_list.append(c_processed)
                    elif id_man.is_valid(norm_id):
                        c_processed["DOI"]= norm_id
                        processed_list.append(c_processed)
                    else:
                        pass

            return processed_list

        elif process_type == "container":
            processed_list = []
            for c in id_dict_list:
                schema = c.get("schema")
                id = c.get("id")
                id_man = self.get_id_manager(schema, self._id_man_dict)
                norm_id = id_man.normalise(id, include_prefix=True)
                if self._redis_db.get(norm_id):
                    processed_list.append(norm_id)
                elif id_man.is_valid(norm_id):
                    processed_list.append(norm_id)
                else:
                    pass
            return processed_list



if __name__ == '__main__':
    arg_parser = ArgumentParser('crossref.py', description='This script preprocesses the tar.gz compressed file of the '
                                                           'crossref dump json files. The ids are validated and the '
                                                           'entities which do not contain citations are discarder. For '
                                                           'each filtered entity dict, only keys which are relevant '
                                                           'for opencitations purposes are kept')
    arg_parser.add_argument('-in', '--input', dest='input', required=True,
                            help=' path to the compressed tar gz input o to the directory containing input json files')
    arg_parser.add_argument('-out_g', '--output_g', dest='output_g', required=True,
                            help='Directory where the preprocessed json files will be stored ')
    arg_parser.add_argument('-n', '--number', dest='number', required=True, type=int,
                            help='Number of relevant entities which will be stored in each  json output file')
    arg_parser.add_argument('-t', '--testing', dest='testing', required=False, type=bool, default=False,
                            help='paremeter to define whether or not the script is executed in testing modality')

    args = arg_parser.parse_args()

    crpp = CrossrefPreProcessing(input_dir=args.input, output_dir=args.output_g,  interval=args.number, testing=args.testing)
    crpp.split_input()

    # HOW TO RUN (example: preprocess) % python -m preprocessing.crossref -in "/Volumes/T7_Touch/LAVORO/COCI/crossref-data-YYYY-MM.tar.gz" -out "/Volumes/T7_Touch/test_preprocess_crossref" -n 100 -t True