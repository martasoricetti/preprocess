import gzip
import json
from os import makedirs
import os
from tqdm import tqdm
import os.path
from os.path import exists
from preprocessing.base import Preprocessing
from oc_idmanager.doi import DOIManager
from oc_idmanager.pmid import PMIDManager
from oc_idmanager.pmcid import PMCIDManager
from datetime import datetime
from argparse import ArgumentParser


class OpenirePreProcessing(Preprocessing):
    """This class aims at pre-processing OpenAire dumps.
    In particular, OpenairePreProcessing opens an input directory containing OpenAIRE TAR files, each
    one containing a directory of .gz scholix files, and iterates over these files in order to filter
    out duplicated citations and citations expressed with identifiers which are not accepted in
    OpenCitations. In the filtering process, the identifiers are also validated against OpenCitations
    Meta REDIS database and - if needed - also using the specific schema API of the identifier. The
    filtered entites are stored in an output directory of .gz scholix files, which is meant to be used
    as input for Meta and Index further processes."""

    _req_type = ".gz"
    _accepted_ids = {"doi", "pmid", "pmc"}
    _accepted_ids_ra = {"orcid"}
    _entity_keys_to_discard = {"dnetIdentifier", "collectedFrom"}
    _entity_keys_to_update = {"identifier", "creator", "publisher"}


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
        self._n = interval
        self._doi_manager = DOIManager()
        self._pmid_manager = PMIDManager()
        self._pmc_manager = PMCIDManager()
        self._id_man_dict = {"doi":self._doi_manager, "pmid": self._pmid_manager, "pmc": self._pmc_manager}

        super(OpenirePreProcessing, self).__init__()

    def split_input(self):
        data = []
        count = 0
        # Iterate over each tar in the directory
        for tar in tqdm(os.listdir(self._input_dir)):
            all_files, targz_fd = self.get_all_files(os.path.join(self._input_dir,tar), self._req_type)
            for file_idx, file in enumerate(tqdm(all_files), 1):
                f = gzip.open(file, 'rb')
                file_content = f.readlines()  # list

                for entity in tqdm(file_content):
                    if entity:
                        d = json.loads(entity.decode('utf-8'))
                        type = (d.get("relationship")).get("name")

                        #filter out all duplicate citations expressed as received citations
                        if type == "Cites":

                            #instantiate the dict for storing reduced and validated version of the citation data
                            validated_dict = dict()

                            citing_data = d.get("source")
                            citing_ids = citing_data.get("identifier")
                            citing_ids_to_keep = []
                            for c_ing_id in citing_ids:

                                # filter out all citing entity ids which are not managed by opencitations
                                if c_ing_id.get("schema").strip().lower() in self._accepted_ids:
                                    citing_ids_to_keep.append(c_ing_id)
                            # filter out all citing entity ids which are not valid
                            citing_ids_to_keep = self.to_validated_id_list(citing_ids_to_keep, "citations")
                            if citing_ids_to_keep:

                                cited_data = d.get("target")
                                cited_ids = cited_data.get("identifier")
                                cited_ids_to_keep = []
                                for c_ed_id in cited_ids:
                                    if c_ed_id.get("schema").strip().lower() in self._accepted_ids:
                                        cited_ids_to_keep.append(c_ed_id)
                                cited_ids_to_keep = self.to_validated_id_list(cited_ids_to_keep, "citations")

                                if cited_ids_to_keep:
                                    # BUILD SOURCE
                                    citing_info = {k:v for (k,v) in d.get("source").items() if k not in self._entity_keys_to_discard and k not in self._entity_keys_to_update}
                                    citing_info["identifier"] = citing_ids_to_keep
                                    if d.get("source").get("publisher"):
                                        citing_info["publisher"] = [i for i in d.get("source").get("publisher")] # MODIFY IF IDS FOR RA ARE PROVIDED
                                    else:
                                        citing_info["publisher"] = []
                                    if d.get("source").get("creator"):
                                        citing_info["creator"] = [i for i in d.get("source").get("creator") ] # MODIFY IF IDS FOR RA ARE PROVIDED
                                    else:
                                        citing_info["creator"] = []

                                    #BUILD TARGET
                                    cited_info = {k:v for (k,v) in d.get("target").items() if k not in self._entity_keys_to_discard and k not in self._entity_keys_to_update}
                                    cited_info["identifier"] = cited_ids_to_keep
                                    if d.get("target").get("publisher"):
                                        cited_info["publisher"] = [i for i in d.get("target").get("publisher")] # MODIFY IF IDS FOR RA ARE PROVIDED
                                    else:
                                        cited_info["publisher"] = []
                                    if d.get("target").get("creator"):
                                        cited_info["creator"] = [i for i in d.get("target").get("creator") ] # MODIFY IF IDS FOR RA ARE PROVIDED
                                    else:
                                        cited_info["creator"] = []

                                    # 1 ADD "relationship" TO REDUCED ENTITY DICT
                                    validated_dict["relationship"] = d.get("relationship")
                                    # 2 ADD "source" TO REDUCED ENTITY DICT
                                    validated_dict["source"] = citing_info
                                    # 3 ADD "target" TO REDUCED ENTITY DICT
                                    validated_dict["target"] = cited_info

                                    # data.append(validated_dict)
                                    data.append(validated_dict)
                                    count += 1
                                    if int(count) != 0 and int(count) % int(self._n) == 0:
                                        data = self.splitted_to_file(count, data, ".gz")

        if len(data) > 0:
            count = count + (self._n - (int(count) % int(self._n)))
            self.splitted_to_file(count, data, ".gz")


    def splitted_to_file(self, cur_n, data, type):
        if int(cur_n) != 0 and int(cur_n) % int(self._n) == 0:
            filename = "filtered_" + str(cur_n // self._n) + self._req_type
            if exists(os.path.join(self._output_dir, filename)):
                cur_datetime = datetime.now()
                dt_string = cur_datetime.strftime("%d%m%Y_%H%M%S")
                filename = filename[:-len(self._req_type)] + "_" + dt_string + self._req_type

            json_str = ("\n".join([json.dumps(x) for x in data]))
            json_bytes = json_str.encode('utf-8')
            with gzip.open(os.path.join(self._output_dir, filename), 'w') as fout:
                fout.write(json_bytes)
            return []
        else:
            return data



    def to_validated_id_list(self, id_dict_list, proc_type):
        """this method takes in input a list of id dictionaries and returns a list valid and existent ids with prefixes.
        For each id, a first validation try is made by checking its presence in META db. If the id is not in META db yet,
        a second attempt is made by using the specific id-schema API"""
        valid_id_list = []
        for ent in id_dict_list:
            schema = ent.get("schema")
            if isinstance(schema, str):
                schema = schema.strip().lower()
            id = ent.get("identifier")
            id_man = self.get_id_manager(schema, self._id_man_dict)
            if id_man:
                norm_id = id_man.normalise(id, include_prefix=True)
                # check if the id is in redis db
                if norm_id:
                    if self._redis_db.get(norm_id):
                        valid_id_list.append(norm_id)
                    # if the id is not in redis db, validate it before appending
                    elif id_man.is_valid(norm_id):
                        valid_id_list.append(norm_id)
        return valid_id_list

if __name__ == '__main__':
    arg_parser = ArgumentParser('openaire.py', description='This script preprocesses a directory of tar compressed '
                                                           'directories containing the gzipped scholix files of the '
                                                           'openaire dump by filtering the duplicated citation entities'
                                                           'by considering the addressed citations only and not the '
                                                           'received, in order to obtain a lighter dump')
    arg_parser.add_argument('-in', '--input', dest='input', required=True,
                            help=' a directory containing the tar compressed directories, containing gz compressed '
                                 'scholix files')
    arg_parser.add_argument('-out_g', '--output_g', dest='output_g', required=True,
                            help='Directory where the preprocessed scholix gz files will be stored ')
    arg_parser.add_argument('-n', '--number', dest='number', required=True, type=int,
                            help='Number of relevant entities which will be stored in each scholix gz file')
    arg_parser.add_argument('-t', '--testing', dest='testing', required=False, type=bool, default=False,
                            help='paremeter to define whether or not the script is executed in testing modality')

    args = arg_parser.parse_args()

    oapp = OpenirePreProcessing(input_dir=args.input, output_dir=args.output_g,  interval=args.number, testing=args.testing)
    oapp.split_input()

#python -m preprocessing.openaire -in "/Volumes/T7_Touch/LAVORO/OROCI/ver_1" -out "/Volumes/T7_Touch/test_preprocess_openaire" -n 500 -t True
