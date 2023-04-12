from os import makedirs
import re
import html
import os.path
from os.path import exists
import json
import csv
from bs4 import BeautifulSoup
import pandas as pd
from preprocessing.base import Preprocessing
from argparse import ArgumentParser
from datetime import datetime
from tqdm import tqdm
from oc_idmanager.doi import DOIManager
from oc_idmanager.pmid import PMIDManager
from preprocessing.finder.nih import NIHResourceFinder


class NIHPreProcessing(Preprocessing):
    """This class aims at pre-processing iCite Database Snapshots (NIH Open
    Citation Collection + ICite Metadata), available at:
    https://nih.figshare.com/search?q=iCite+Database+Snapshot. In particular,
    NIHPreProcessing splits the original CSV file in many lighter CSV files,
    each one containing the number of entities specified in input by the user.
    In addition to that, csv fields which contain information which is not
    relevant for OpenCitations purposes are discarded, and also the entities
    that are not involved in citations are excluded from the output files."""
    _req_type = ".csv"
    _accepted_ids = {"doi", "pmid"}
    _entity_keys_to_discard = {"is_research_article","citation_count","field_citation_rate","expected_citations_per_year","citations_per_year","relative_citation_ratio","nih_percentile","human","animal","molecular_cellular","x_coord","y_coord","apt","is_clinical","cited_by_clin","provisional"}
    _entity_keys_to_update = {"pmid", "doi", "cited_by","references"}
    _entity_keys_to_keep = {"pmid","title","authors","year","journal","cited_by","references"}
    _filter = ["pmid", "doi", "title", "authors", "year", "journal", "cited_by", "references"]

    def __init__(self, input_dir, output_dir, interval, journals_dict_path, testing=False):
        self.journals_dict_path = journals_dict_path
        self.jour_dict = self.issn_data_recover_poci(journals_dict_path)
        if testing:
            self._redis_db = self.BR_redis_test
        else:
            self._redis_db = self.BR_redis
        self._input_dir = input_dir
        self._output_dir = output_dir
        if not exists(self._output_dir):
            makedirs(self._output_dir)
        self._interval = interval
        self._doi_manager = DOIManager()
        self._pmid_manager = PMIDManager()
        self._nih_rf = NIHResourceFinder()
        self._id_man_dict = {"doi":self._doi_manager, "pmid": self._pmid_manager}

        super(NIHPreProcessing, self).__init__()

    def issn_data_recover_poci(self, path):
        journal_issn_dict = dict()
        if not os.path.exists(path):
            return journal_issn_dict
        else:
            with open(path, "r", encoding="utf8") as fd:
                journal_issn_dict = json.load(fd)
                return journal_issn_dict

    def issn_data_to_cache_poci(self, jtitle_issn_dict, path):
        with open(path, "w", encoding="utf-8") as fd:
            json.dump(jtitle_issn_dict, fd, ensure_ascii=False, indent=4)

    def splitted_to_file(self, cur_n, lines, type=None):
        if int(cur_n) != 0 and int(cur_n) % int(self._interval) == 0:
            # to be logged: print("Processed lines:", cur_n, ". Reduced csv nr.", cur_n // self._interval)
            filename = "CSVFile_" + str(cur_n // self._interval) + self._req_type
            if exists(os.path.join(self._output_dir, filename)):
                cur_datetime = datetime.now()
                dt_string = cur_datetime.strftime("%d%m%Y_%H%M%S")
                filename = filename[:-len(self._req_type)] + "_" + dt_string + self._req_type

            with open(os.path.join(self._output_dir, filename), "w", encoding="utf8", newline="") as f_out:
                keys = self._filter
                dict_writer = csv.DictWriter(f_out, delimiter=",", quoting=csv.QUOTE_ALL, escapechar="\\", fieldnames=keys)
                dict_writer.writeheader()
                dict_writer.writerows(lines)

            lines = []
            return lines
        else:
            return lines


    def split_input(self):
        all_files, targz_fd = self.get_all_files(self._input_dir, self._req_type)
        count = 0
        lines = []

        for file_idx, file in enumerate(tqdm(all_files), 1):
            chunksize = 100000
            with pd.read_csv(file,  usecols=self._filter, chunksize=chunksize) as reader:
                for chunk in reader:
                    chunk.fillna("", inplace=True)
                    df_dict_list = chunk.to_dict("records")
                    filt_values = [d for d in df_dict_list if (d.get("cited_by") or d.get("references"))]

                    for line in filt_values:
                        count += 1
                        norm_pmid = self.to_validated_id_list([{"id":line.get("pmid"), "schema":"pmid"}], "citations")[0]
                        if norm_pmid is None:
                            print("ERROR:", line.get("pmid"), norm_pmid)
                            continue
                        ref = line.get("references").split() if line.get("references") else []
                        cit_by = line.get("cited_by").split() if line.get("cited_by") else []
                        if ref:
                            ref_dict_list = [{"id":i, "schema":"pmid"} for i in ref]
                            norm_ref = ' '.join([x for x in self.to_validated_id_list(ref_dict_list, "citations") if x]).strip()
                        else:
                            norm_ref = ""
                        if cit_by:
                            cit_by_dict_list = [{"id":i, "schema":"pmid"} for i in cit_by]
                            norm_cit_by = ' '.join([x for x in self.to_validated_id_list(cit_by_dict_list, "citations") if x]).strip()
                        else:
                            norm_cit_by = ""
                        valid_doi = None
                        if line.get("doi"):
                            valid_doi = self.to_validated_id_list([{"id":line.get("doi"), "schema":"doi"}], "citations")[0]
                        if not valid_doi:
                            valid_doi = ""
                        valid_venue, self.jour_dict = self.get_venue_title_and_id(line.get("journal"), self.jour_dict, norm_pmid)

                        line["pmid"] = norm_pmid
                        line["doi"] = valid_doi
                        line["journal"] = valid_venue
                        line["references"] = norm_ref
                        line["cited_by"] = norm_cit_by

                        lines.append(line)
                        if int(count) != 0 and int(count) % int(self._interval) == 0:
                            lines = self.splitted_to_file(count, lines)
                            self.issn_data_to_cache_poci(self.jour_dict, self.journals_dict_path)

        if len(lines) > 0:
            count = count + (self._interval - (int(count) % int(self._interval)))
            self.splitted_to_file(count, lines)
            self.issn_data_to_cache_poci(self.jour_dict, self.journals_dict_path)

    def to_validated_id_list(self, id_dict_list, process_type):
        if process_type == "citations":
            valid_id_list = []
            for i in id_dict_list:
                id = i.get("id")
                if id:
                    schema = i.get("schema")
                    id_man = self.get_id_manager(schema, self._id_man_dict)
                    if id_man:
                        norm_id = id_man.normalise(id, include_prefix=True)
                        if norm_id:
                            if schema == "pmid":
                                # since the datasource is also responsible for the only pmid API, there is no need of
                                # further checks on the ids validity
                                valid_id_list.append(norm_id)

                            else:
                                # check if the id is in redis db
                                if self._redis_db.get(norm_id):
                                    valid_id_list.append(norm_id)
                                # if the id is not in redis db, validate it before appending
                                elif id_man.is_valid(norm_id):
                                    valid_id_list.append(norm_id)
                                else:
                                    valid_id_list.append(None)
                        else:
                            valid_id_list.append(None)
                    else:
                        valid_id_list.append(None)

            return valid_id_list

    def get_venue_title_and_id(self, short_n, jour_dict, be_id):
        """
        :param short_n: 
        :param jour_dict: 
        :param be_id: 
        :return: 
        """"""
        This method deals with generating the venue's name, followed by id in square brackets, separated by spaces.
        HTML tags are deleted and HTML entities escaped. In addition, any ISBN and ISSN are validated.
        Finally, the square brackets in the venue name are replaced by round brackets to avoid conflicts with the ids 
        enclosures.

        :params item: the item's dictionary
        :type item: dict
        :params row: a CSV row
        :type row: dict
        :returns: str, dict -- The output is a string in the format 'NAME [SCHEMA:ID]', for example, 
        'Nutrition & Food Science [issn:0034-6659]', and the updated dictionary mapping short journal 
        names to extended journal names and ISSN ids, If the id does not exist, the output is only the name. 
        Finally, if there is no venue, the output is an empty string. 
        """

        cont_title = ""
        venids_list = []
        if short_n:
            if short_n not in self.jour_dict.keys():
                jour_dict[short_n] = {"extended": "", "issn": []}
            if not jour_dict[short_n].get("extended") or not jour_dict[short_n].get("issn"):
                if be_id:
                    api_response = self._nih_rf._call_api(be_id)
                    if api_response:
                        if not jour_dict[short_n].get("extended"):
                            jour_dict[short_n]["extended"] = self._nih_rf._get_extended_j_title(api_response)
                        if not jour_dict[short_n].get("issn"):
                            issn_dict_list_valid = [x for x in self._nih_rf._get_issn(api_response) if x]
                            jour_dict[short_n]["issn"] = issn_dict_list_valid

            if short_n in self.jour_dict.keys():
                jt_data = self.jour_dict.get(short_n)
                if jt_data.get("issn"):
                    venids_list = [x for x in jt_data.get("issn")]
                extended_jt = jt_data.get("extended") if jt_data.get("extended") else short_n
                cont_title = extended_jt

        # use abbreviated journal title if no mapping was provided
        cont_title = cont_title.replace('\n', '')
        ven_soup = BeautifulSoup(cont_title, 'html.parser')
        ventit = html.unescape(ven_soup.get_text())
        ambiguous_brackets = re.search('\[\s*((?:[^\s]+:[^\s]+)?(?:\s+[^\s]+:[^\s]+)*)\s*\]', ventit)
        if ambiguous_brackets:
            match = ambiguous_brackets.group(1)
            open_bracket = ventit.find(match) - 1
            close_bracket = ventit.find(match) + len(match)
            ventit = ventit[:open_bracket] + '(' + ventit[open_bracket + 1:]
            ventit = ventit[:close_bracket] + ')' + ventit[close_bracket + 1:]
            cont_title = ventit

            # IDS
        if venids_list:
            name_and_id = cont_title + ' [' + ' '.join(venids_list) + ']' if cont_title else '[' + ' '.join(venids_list) + ']'
        else:
            name_and_id = cont_title

        return name_and_id, self.jour_dict

if __name__ == '__main__':
    arg_parser = ArgumentParser('pubmed.py', description='This script preprocesses a NIH dump (iCite Metadata) by '
                                                         'discarding the entities which are not involved in citations '
                                                         'and storing the other ones in smaller csv files, containing'
                                                         'a reduced number of fields with respect to the original'
                                                         'csv file')
    arg_parser.add_argument('-in', '--input', dest='input', required=True,
                            help='Either a directory containing the decompressed csv input file or the zip compressed '
                                 'csv input file')
    arg_parser.add_argument('-out', '--output', dest='output', required=True,
                            help='Directory the preprocessed CSV files will be stored')
    arg_parser.add_argument('-n', '--number', dest='number', required=True, type=int,
                            help='Number of relevant entities which will be stored in each CSV file')
    arg_parser.add_argument('-jtp', '--jtpath', dest='jtpath', required=True, type=str,
                            help='path to the json file mapping journal titles abbreviations to the extended'
                                 'journal titles and to their ISSNs. If a pre-processed json file can be found'
                                 'at the specified file path, it is loaded in a python dictionary, otherwise a '
                                 'new json file is created at the specified location')
    arg_parser.add_argument('-t', '--testing', dest='testing', required=False, type=bool, default=False,
                            help='paremeter to define whether or not the script is executed in testing modality')

    args = arg_parser.parse_args()


    nihpp = NIHPreProcessing(input_dir=args.input, output_dir=args.output, interval=args.number, journals_dict_path=args.jtpath, testing=args.testing)
    nihpp.split_input()


    # HOW TO RUN (example: preprocess) % python -m preprocessing.pubmed -in "/Volumes/T7_Touch/LAVORO/POCI/MAR_23_post/icite_metadata.zip" -out "/Volumes/T7_Touch/test_preprocess_nih" -n 100 -jtp "/Volumes/T7_Touch/LAVORO/POCI/journal_issn_extt.json" -t True
