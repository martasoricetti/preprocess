
from preprocessing.base import Preprocessing
from os import makedirs
from oc_idmanager.doi import DOIManager
from oc_idmanager.issn import ISSNManager
from preprocessing.identifier_manager.jid import JIDManager
from os.path import exists
import os
import datetime
import json
from tqdm import tqdm
from datetime import datetime
from argparse import ArgumentParser


class JalcPreProcessing(Preprocessing):
    _req_type = ".json"
    _accepted_ids = "doi"
    _accepted_ids_venue = {"issn", "jid"}
    _entity_keys_to_discard = {"relation_list", "keyword_list", "sequence", "affiliation_list", "original_text", "url"}
    _entity_keys_to_update = {"citation_list", "creator_list", "journal_id_list"}

    def __init__(self, input_dir, output_dir, interval, testing=False):
        if testing:
            self._redis_db = self.BR_redis_test
        else:
            self._redis_db = self.BR_redis
        self._input_dir = input_dir
        self._output_dir = output_dir
        if not exists(self._output_dir):
            makedirs(self._output_dir)
        self._n = interval
        self._doi_manager = DOIManager()
        self._issn_manager = ISSNManager()
        self._jid_manager = JIDManager()
        self._id_man_dict = {"doi" :self._doi_manager, "issn": self._issn_manager, "jid": self._jid_manager}

        super(JalcPreProcessing, self).__init__()

    def to_validated_id_list(self, id_dict_list, process_type):
        if process_type == "venue":
            norm_identifiers = []
            for v in id_dict_list:
                if v.get("journal_id"):
                    if v.get("type").lower().strip() in self._accepted_ids_venue:
                        schema = v.get("type").lower().strip()
                        id = v.get("journal_id")
                        id_man = self.get_id_manager(schema, self._id_man_dict)
                        if id_man:
                            norm_id = id_man.normalise(id, include_prefix=True)
                            # check if the id is in redis db
                            if self._redis_db.get(norm_id):
                                norm_identifiers.append(norm_id)
                            # if the id is not in redis db, validate it before appending
                            elif id_man.is_valid(norm_id):
                                norm_identifiers.append(norm_id)
            return norm_identifiers
        if process_type == "citation":
            processed_list = []
            norm_identifiers = []
            for c in id_dict_list:
                if c.get("doi"):
                    schema = "doi"
                    id = c.get("doi")
                    id_man = self.get_id_manager(schema, self._id_man_dict)
                    if id_man:
                        norm_id = id_man.normalise(id, include_prefix=True)
                        # check if the id is in redis db
                        if self._redis_db.get(norm_id):
                            norm_identifiers.append(norm_id)
                        # if the id is not in redis db, validate it before appending
                        elif id_man.is_valid(norm_id):
                            norm_identifiers.append(norm_id)
                        if norm_id in norm_identifiers:
                            citation_processed_dict = dict()
                            citation_processed_dict["doi"] = norm_id
                            citation_processed_dict.update({k: v for (k, v) in c.items() if k not in self._entity_keys_to_discard and k not in self._entity_keys_to_update and k not in
                                                                {"doi"}})
                            if c.get("creator_list"):
                                cited_entities_creators = []
                                for author in c.get("creator_list"):
                                    creator = {k: v for (k, v) in author.items() if
                                               k not in self._entity_keys_to_discard}
                                    cited_entities_creators.append(creator)
                                citation_processed_dict["creator_list"] = cited_entities_creators
                            processed_list.append(citation_processed_dict)
                        else:
                            pass
            return processed_list
        # in this case id_dict_list is just a single doi
        if process_type == "citing_entity":
            schema = "doi"
            id_man = self.get_id_manager(schema, self._id_man_dict)
            if id_man:
                norm_id = id_man.normalise(id_dict_list, include_prefix=True)
                # check if the id is in redis db
                if self._redis_db.get(norm_id):
                    return norm_id
                # if the id is not in redis db, validate it before appending
                elif id_man.is_valid(norm_id):
                    return norm_id
                else:
                    return None

    def splitted_to_file(self, cur_n, data, type):
        if type == ".ndjson":
            if int(cur_n) != 0 and int(cur_n) % int(self._n) == 0:
                filename = "filtered_" + str(cur_n // self._n) + ".ndjson"
                if exists(os.path.join(self._output_dir, filename)):
                    cur_datetime = datetime.now()
                    dt_string = cur_datetime.strftime("%d%m%Y_%H%M%S")
                    filename = filename[:-len(".ndjson")] + "_" + dt_string + ".ndjson"
                with open(os.path.join(self._output_dir, filename), "w", encoding="utf8") as f_out:
                    # Concatenate dictionaries in list "data" with '\n' separator
                    json_str = '\n'.join([json.dumps(x, ensure_ascii=False) for x in data])
                    f_out.write(json_str)
                    f_out.close()
                return []
            else:
                return data
        else:
            return data

    def split_input(self):
        # an empty list to store the filtered entities to be saved in the output files is created
        data = []
        count = 0
        # iterate over the input data
        all_files, targz_fd = self.get_all_files(self._input_dir, ".zip")
        for i, el in enumerate(tqdm(all_files), 1):
            if el:
                all_files_unzipped, targz_fd_el = self.get_all_files(el, self._req_type)
                for file_idx, file in enumerate(tqdm(all_files_unzipped), 1):
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
                            citing_id_to_keep = self.to_validated_id_list(d.get("doi"), "citing_entity")
                            if citing_id_to_keep:
                                # start creating reduced entity file
                                entity_data = dict()
                                entity_data["doi"] = citing_id_to_keep
                                entity_data.update({k: v for (k, v) in d.items() if
                                                    k not in self._entity_keys_to_discard and k not in self._entity_keys_to_update and k not in {
                                                        "doi"}})
                                # journal_id_list
                                if d.get("journal_id_list"):
                                    venues = d.get("journal_id_list")
                                    processed_venues = self.to_validated_id_list(venues, "venue")
                                    entity_data["journal_id_list"] = processed_venues
                                # creator_list
                                if d.get("creator_list"):
                                    entity_data["creator_list"] = []
                                    for x in d.get("creator_list"):
                                        creator = {k: v for (k, v) in x.items() if
                                                   k not in self._entity_keys_to_discard and k not in self._entity_keys_to_update}
                                        entity_data["creator_list"].append(creator)
                                # citation_list
                                citations = d.get("citation_list")
                                processed_citations = self.to_validated_id_list(citations, "citation")
                                entity_data["citation_list"] = processed_citations

                                data.append(entity_data)
                                count += 1
                                if int(count) != 0 and int(count) % int(self._n) == 0:
                                    data = self.splitted_to_file(count, data, ".ndjson")
                    f.close()
        if len(data) > 0:
            count = count + (self._n - (int(count) % int(self._n)))
            self.splitted_to_file(count, data, ".ndjson")


if __name__ == '__main__':
    arg_parser = ArgumentParser('jalc.py',
                                description='This script preprocesses a directory of zip compressed directories containing'
                                            'the json files of the jalc dump by filtering the bibliographic entities without citations and'
                                            'the ones without dois of cited entities and storing the others in smaller json files')
    arg_parser.add_argument('-in', '--input', dest='input', required=True,
                            help=' a directory containing the tar compressed directories, containing gz compressed scholix files')
    arg_parser.add_argument('-out_g', '--output_g', dest='output_g', required=True,
                            help='Directory where the preprocessed scholix gz files will be stored ')
    arg_parser.add_argument('-n', '--number', dest='number', required=True, type=int,
                            help='Number of relevant entities which will be stored in each scholix gz file')
    arg_parser.add_argument('-t', '--testing', dest='testing', required=False, type=bool, default=False,
                            help='paremeter to define whether or not the script is executed in testing modality')

    args = arg_parser.parse_args()

    japp = JalcPreProcessing(input_dir=args.input, output_dir=args.output_g, interval=args.number, testing=args.testing)
    japp.split_input()
