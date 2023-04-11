import ndjson
from os import makedirs, listdir
import glob
import json
import os
from tqdm import tqdm
import os.path
from os.path import exists, join
from preprocessing.base import Preprocessing
from oc_idmanager.pmid import PMIDManager
from oc_idmanager.pmcid import PMCIDManager
from oc_idmanager.doi import DOIManager
from oc_idmanager.orcid import ORCIDManager
from oc_idmanager.viaf import ViafManager
from oc_idmanager.issn import ISSNManager
from oc_idmanager.isbn import ISBNManager
from oc_idmanager.ror import RORManager
from oc_idmanager.wikidata import WikidataManager
from datetime import datetime
from argparse import ArgumentParser

class DatacitePreProcessing(Preprocessing):
    """This class aims at pre-processing DataCite dumps.
    In particular, DatacitePreProcessing splits the original ndJSON in many ndJSON files, each one containing the
    number of entities specified in input by the user. Further, the class validates identifiers, and discards those
    entities that are not involved in citations, together with key-value pairs which are not used in OpenCitations
    processes."""

    _req_type = ".ndjson"
    _accepted_ids = {"doi", "pmid", "pmcid", "wikidata"}
    _accepted_ids_ra = {"orcid", "viaf", "ror"}
    _entity_keys_to_keep = {"titles", "publicationYear", "dates", "types", "updated", "publisher"}
    _entity_keys_to_update = {"identifiers", "creators", "container", "contributors", "relatedIdentifiers"}
    _container_id_map = {"Series": ["doi","issn"], "Journal": ["doi","issn"], "Book Series": ["doi","issn"], "DataRepository": ["doi", "issn"], "ISSN": ["issn"], "Book": ["doi","isbn"]}
    _accepted_ids_container = {"doi", "issn", "isbn"}
    # all keys in the origial dump = "doi", "identifiers", "creators", "titles", "publisher", "container",
    # "contributors", "dates", "language", "types", "relatedIdentifiers", "sizes", "formats", "version", "rightsList",
    # "descriptions", "geoLocations", "fundingReferences", "url", "contentUrl", "metadataVersion", "schemaVersion",
    # "source", "isActive", "state", "reason", "viewCount", "downloadCount", "referenceCount", "citationCount",
    # "partCount", "partOfCount", "versionCount", "versionOfCount", "created", "registered", "published", "updated"
    # "publicationYear", "subjects",

    def __init__(self, input_dir, output_dir, interval, testing=False):
        if testing:
            self._redis_db = self.BR_redis_test
            self._redis_db_ra = self.RA_redis_test
        else:
            self._redis_db = self.BR_redis
            self._redis_db_ra = self.RA_redis
        self._doi_manager = DOIManager()
        self._issn_manager = ISSNManager()
        self._isbn_manager = ISBNManager()
        self._orcid_manager = ORCIDManager()
        self._viaf_manager = ViafManager()
        self._ror_manager = RORManager()
        self._wikidata_manager = WikidataManager()
        self._pmid_manager = PMIDManager()
        self._pmcid_manager = PMCIDManager()
        self._wikidata_manager = WikidataManager()
        self._input_dir = input_dir
        self._output_dir = output_dir
        self._needed_info = ["relationType", "relatedIdentifierType", "relatedIdentifier"]
        self._id_man_dict = {"doi":self._doi_manager, "pmcid": self._pmcid_manager, "pmid":self._pmid_manager, "wikidata": self._wikidata_manager, "ror": self._ror_manager, "issn": self._issn_manager, "viaf": self._viaf_manager, "isbn": self._isbn_manager, "orcid": self._orcid_manager}



        if not exists(self._output_dir):
            makedirs(self._output_dir)
        self._interval = interval
        self._cites_filter = ["references", "cites"]
        self._citedby_filter = ["isreferencedby", "iscitedby"]
        self._csv_col = ["citing", "referenced"]
        super(DatacitePreProcessing, self).__init__()

    def split_input(self):

        all_files, targz_fd = self.get_all_files(self._input_dir, self._req_type)
        data = []
        count = 0

        # PROCESS START (on files)
        for file_idx, file in enumerate(tqdm(all_files), 1):
            f = open(file, encoding="utf8")
            for line in tqdm(f):
                if line:
                    try:
                        linedict = json.loads(line)
                    except:
                        print(ValueError, line)
                        continue

                    # PROCESS START (on entities)
                    if linedict:
                        d = linedict["data"]
                        for e in d:
                            if 'id' not in e or 'type' not in e:
                                continue
                            doi_entity = self._doi_manager.normalise(e['id'])
                            if doi_entity:
                                if e['type'] == "dois":
                                    attributes = e["attributes"]

                                    # add k,v pairs which do not need to be modified: "titles", "publicationYear",
                                    # "dates", "types", "updated", "publisher"
                                    processed_entity = {k:v for k,v in e.get("attributes").items() if k in self._entity_keys_to_keep}

                                    # add an updated version of the k,v pairs which needs some check or validation
                                    #doi
                                    processed_entity["doi"] = doi_entity

                                    # relatedIdentifiers
                                    rel_ids = attributes.get("relatedIdentifiers")
                                    if rel_ids:

                                        cites_ents, citedby_ents, rel_container = self.to_validated_id_list(rel_ids, "related_ids")

                                        # In order to avoid validating data for entities which are not going to be included
                                        # because they are not involved in any citations, the related identifiers are
                                        # checked first, and the process prosecutes only if any related id was found.
                                        if cites_ents or citedby_ents:
                                            valid_rel_id_dict = dict()
                                            valid_rel_id_dict["Cites"] = cites_ents
                                            valid_rel_id_dict["IsCitedBy"] = citedby_ents
                                            valid_rel_id_dict["IsPartOf"] = rel_container
                                            processed_entity["relatedIdentifiers"] = valid_rel_id_dict

                                            # contributors
                                            contribs = attributes.get("contributors")
                                            processed_contribs =[]
                                            if contribs:
                                                processed_contribs = self.to_validated_id_list(contribs, "contributors")
                                            processed_entity["contributors"] = processed_contribs

                                            # creators
                                            creators = attributes.get("creators")
                                            processed_creators =[]
                                            if creators:
                                                processed_creators = self.to_validated_id_list(creators, "creators")
                                            processed_entity["creators"] = processed_creators

                                            # identifiers
                                            ids = attributes.get("identifiers")
                                            processed_ids = []
                                            if ids:
                                                processed_ids = self.to_validated_id_list(ids, "identifiers")
                                            processed_entity["identifiers"] = processed_ids

                                            # container
                                            container = attributes.get("container") #one dict
                                            processed_container = dict()
                                            if container:
                                                processed_container = self.to_validated_id_list([container], "container")
                                            processed_entity["container"] = processed_container

                                            data.append(processed_entity)
                                            count += 1
                                            if int(count) != 0 and int(count) % int(self._interval) == 0:
                                                data = self.splitted_to_file(count, data, ".ndjson")

            f.close()
        if len(data) > 0:
            count = count + (self._interval - (int(count) % int(self._interval)))
            self.splitted_to_file(count, data, ".ndjson")

    def splitted_to_file(self, cur_n, data, type):
        if type == ".ndjson":
            if int(cur_n) != 0 and int(cur_n) % int(self._interval) == 0: # and len(data)
                filename = "jSonFile_" + str(cur_n // self._interval) + self._req_type
                if exists(os.path.join(self._output_dir, filename)):
                    cur_datetime = datetime.now()
                    dt_string = cur_datetime.strftime("%d%m%Y_%H%M%S")
                    filename = filename[:-len(self._req_type)] + "_" + dt_string + self._req_type
                with open(os.path.join(self._output_dir, filename), "w", encoding="utf8") as json_file:
                    # Concatenate dictionaries in list "data" with '\n' separator
                    ndjson = '\n'.join([json.dumps(d, ensure_ascii=False) for d in data])
                    # Write to file
                    json_file.write(ndjson)
                    json_file.close()

                return []
            else:
                return data
        else:
            return data

    def to_validated_id_list(self, id_dict_list, process_type):
        if process_type == "contributors" or process_type == "creators":
            processed_list = []
            for c in id_dict_list:
                norm_identifiers = []
                if (c.get("givenName") and c.get("familyName")) or c.get("name"):
                    proceed = True
                    if process_type == "contributors":
                        if c.get("contributorType"):
                            if c.get("contributorType") != "Editor":
                                proceed = False
                    if proceed:
                        if c.get("nameIdentifiers"):
                            name_ids = c.get("nameIdentifiers")
                            for nid in name_ids:
                                if nid.get("nameIdentifierScheme"):
                                    if nid.get("nameIdentifierScheme").lower().strip() in self._accepted_ids_ra:
                                        schema = nid.get("nameIdentifierScheme").lower().strip()
                                        id = nid.get("nameIdentifier")
                                        id_man = self.get_id_manager(schema, self._id_man_dict)
                                        if id_man:
                                            norm_id = id_man.normalise(id, include_prefix=True)
                                            if norm_id:
                                                if self._redis_db_ra.get(norm_id):
                                                    norm_identifiers.append(norm_id)
                                                elif id_man.is_valid(norm_id):
                                                    norm_identifiers.append(norm_id)
                                                else:
                                                    pass

                    contrib_processed_dict = dict()
                    if process_type == "contributors":
                        contrib_processed_dict = {k:v for k,v in c.items() if k in {"givenName", "familyName", "name", "contributorType", "nameType"}}
                    elif process_type == "creators":
                        contrib_processed_dict = {k:v for k,v in c.items() if k in {"givenName", "familyName", "name", "nameType"}}

                    contrib_processed_dict["nameIdentifiers"] = norm_identifiers
                    processed_list.append(contrib_processed_dict)

            return processed_list

        elif process_type == "identifiers":

            processed_list = []
            for c in id_dict_list:
                i_type = c.get("identifierType")
                if i_type:
                    if i_type.lower().strip() in self._accepted_ids and i_type.lower().strip() != "doi":
                        # we assume that a doi here is either a repetition or an error
                        schema = i_type.lower().strip()
                        id = c.get("identifier")
                        id_man = self.get_id_manager(schema, self._id_man_dict)
                        if id_man:
                            norm_id = id_man.normalise(id, include_prefix=True)
                            if norm_id:
                                if self._redis_db.get(norm_id):
                                    processed_list.append(norm_id)
                                elif id_man.is_valid(norm_id):
                                    processed_list.append(norm_id)
                                else:
                                    pass

            return processed_list

        elif process_type == "container":

            dict_input = id_dict_list[0]
            to_keep = {'type', 'title', 'firstPage', 'volume', 'issue', 'lastPage'}
            processed_dict = {k:v for k,v in dict_input.items() if k in to_keep}
            i_type = dict_input.get("identifierType")
            cont_type = processed_dict.get('type') if processed_dict.get('type') and processed_dict.get('type') != 'null' else None
            if i_type and cont_type:
                if self._container_id_map.get(cont_type):
                    #  {'type': 'Series', 'identifier': '10.11646/zootaxa.4059.3.1', 'identifierType': 'DOI'}
                    if i_type.lower().strip() in self._container_id_map[cont_type]:
                        # we assume that a doi is either a repetition or an error
                        schema = i_type.lower().strip()
                        id = dict_input.get("identifier")
                        id_man = self.get_id_manager(schema, self._id_man_dict)
                        if id_man:
                            norm_id = id_man.normalise(id, include_prefix=True)
                            if norm_id:
                                if self._redis_db.get(norm_id):
                                    processed_dict["identifier"] = [norm_id]
                                    return processed_dict
                                elif id_man.is_valid(norm_id):
                                    processed_dict["identifier"] = [norm_id]
                                    return processed_dict
                                else:
                                    processed_dict["identifier"] = []
                                    return processed_dict
                            else:
                                processed_dict["identifier"] = []
                                return processed_dict


            # return the input dict without the keys "identifier" and "identifierType" in the case the id was not valid
            processed_dict["identifier"] = []
            return processed_dict

        elif process_type == "related_ids":

            valid_id_list_cites = []
            valid_id_list_citedby = []
            valid_id_container = []

            for ref in id_dict_list:
                if all(ref.get(elem) for elem in self._needed_info):
                    schema = (str(ref["relatedIdentifierType"])).lower().strip()
                    id_man = self.get_id_manager(schema, self._id_man_dict)
                    relationType = str(ref["relationType"]).lower().strip()

                    if id_man:
                        norm_id = id_man.normalise(str(ref["relatedIdentifier"]), include_prefix=True)
                        if norm_id:
                            if relationType == "references" or relationType == "cites":
                                if norm_id not in valid_id_list_cites:
                                    # check if the id is in redis db
                                    if self._redis_db.get(norm_id):
                                        valid_id_list_cites.append(norm_id)
                                    # if the id is not in redis db, validate it before appending
                                    elif id_man.is_valid(norm_id):
                                        valid_id_list_cites.append(norm_id)
                                    else:
                                        pass

                            elif relationType == "isreferencedby" or relationType == "iscitedby":
                                if norm_id not in valid_id_list_citedby:
                                    # check if the id is in redis db
                                    if self._redis_db.get(norm_id):
                                        valid_id_list_citedby.append(norm_id)
                                    # if the id is not in redis db, validate it before appending
                                    elif id_man.is_valid(norm_id):
                                        valid_id_list_citedby.append(norm_id)
                                    else:
                                        pass

                            elif relationType == "ispartof":
                                if schema in self._accepted_ids_container:
                                    if norm_id not in valid_id_container:
                                        # check if the id is in redis db
                                        if self._redis_db.get(norm_id):
                                            valid_id_container.append(norm_id)
                                        # if the id is not in redis db, validate it before appending
                                        elif id_man.is_valid(norm_id):
                                            valid_id_container.append(norm_id)
                                        else:
                                            pass

            return valid_id_list_cites, valid_id_list_citedby, valid_id_container


if __name__ == '__main__':
    arg_parser = ArgumentParser('datacite.py', description='This script preprocesses a nldjson datacite dump by '
                                                              'deleting the entities which are not involved in citations'
                                                              'and storing the other ones in smaller json files')
    arg_parser.add_argument('-in', '--input', dest='input', required=True,
                            help='Either a directory containing the decompressed json input file or the zst compressed '
                                 'json input file')
    arg_parser.add_argument('-out', '--output', dest='output_g', required=True,
                            help='Directory where the preprocessed json files will be stored')
    arg_parser.add_argument('-n', '--number', dest='number', required=True, type=int,
                            help='Number of relevant entities which will be stored in each json file')
    arg_parser.add_argument('-t', '--testing', dest='testing', required=False, type=bool, default=False,
                            help='paremeter to define whether or not the script is executed in testing modality')

    args = arg_parser.parse_args()


    dcpp = DatacitePreProcessing(input_dir=args.input, output_dir=args.output_g,  interval=args.number, testing=args.testing)
    dcpp.split_input()

    # HOW TO RUN (example: preprocess) % python -m preprocessing.datacite -in "/Volumes/T7_Touch/LAVORO/DOCI/dump2022/datacite_dump_20221118.ndjson.zst" -out "/Volumes/T7_Touch/test_preprocess_datacite" -n 100 -t True