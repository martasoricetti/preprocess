import os.path
from abc import ABCMeta, abstractmethod
from os import sep, makedirs, walk
from os.path import exists, basename, isdir
import tarfile
from json import load, loads
import zstandard as zstd
import pathlib
import zipfile
import fakeredis
from preprocessing.datasource.redis import RedisDataSource


class Preprocessing(metaclass=ABCMeta):
    """This is the interface for implementing preprocessors for specific datasources.
    It provides the signatures of the methods for preprocessing a dump"""
    BR_redis_test = fakeredis.FakeStrictRedis()
    BR_redis = RedisDataSource("DB-META-BR")
    RA_redis_test = fakeredis.FakeStrictRedis()
    RA_redis = RedisDataSource("DB-META-RA")

    def __init__(self, **params):
        """preprocessor constructor."""
        for key in params:
            setattr(self, key, params[key])

    def get_all_files(self, i_dir_or_compr, req_type):
        """This method is meant to extract all the files of a given format from the compressed file
        or directory at the filepath in input. It takes in input a filepath and the string of the
        required filename extension, such as '.csv', '.json', '.ndjson', etc. The output includes
        a list of extracted files and a targz opener, if required."""
        result = []
        targz_fd = None

        if isdir(i_dir_or_compr):

            for cur_dir, cur_subdir, cur_files in walk(i_dir_or_compr):
                for cur_file in cur_files:
                    if cur_file.endswith(req_type) and not basename(cur_file).startswith("."):
                        result.append(os.path.join(cur_dir, cur_file))
        elif i_dir_or_compr.endswith("tar.gz"):
            targz_fd = tarfile.open(i_dir_or_compr, "r:gz", encoding="utf-8")
            for cur_file in targz_fd:
                if cur_file.name.endswith(req_type) and not basename(cur_file.name).startswith("."):
                    result.append(cur_file)
        elif i_dir_or_compr.endswith("zip"):
            with zipfile.ZipFile(i_dir_or_compr, 'r') as zip_ref:
                dest_dir = ".".join(i_dir_or_compr.split(".")[:-1]) + "_decompr_zip_dir"
                if not exists(dest_dir):
                    makedirs(dest_dir)
                zip_ref.extractall(dest_dir)
            for cur_dir, cur_subdir, cur_files in walk(dest_dir):
                for cur_file in cur_files:
                    if cur_file.endswith(req_type) and not basename(cur_file).startswith("."):
                        result.append(cur_dir + sep + cur_file)

        elif i_dir_or_compr.endswith("zst"):
            input_file = pathlib.Path(i_dir_or_compr)
            dest_dir = ".".join(i_dir_or_compr.split(".")[:-1]) + "_decompr_zst_dir"
            with open(input_file, 'rb') as compressed:
                decomp = zstd.ZstdDecompressor()
                if not exists(dest_dir):
                    makedirs(dest_dir)
                output_path = pathlib.Path(dest_dir) / input_file.stem
                if not exists(output_path):
                    with open(output_path, 'wb') as destination:
                        decomp.copy_stream(compressed, destination)
            for cur_dir, cur_subdir, cur_files in walk(dest_dir):
                for cur_file in cur_files:
                    if cur_file.endswith(req_type) and not basename(cur_file).startswith("."):
                        result.append(cur_dir + sep + cur_file)
        elif i_dir_or_compr.endswith(".tar"):
            dest_dir = ".".join(i_dir_or_compr.split(".")[:-1]) + "_open_tar_dir"
            with tarfile.open(i_dir_or_compr, "r") as tf:
                tf.extractall(path=dest_dir)
            for cur_dir, cur_subdir, cur_files in walk(dest_dir):
                for cur_file in cur_files:
                    if cur_file.endswith(req_type) and not basename(cur_file).startswith("."):
                        result.append(cur_dir + sep + cur_file)
        else:
            print("It is not possible to process the input path.", i_dir_or_compr)
        return result, targz_fd

    def load_json(self, file, targz_fd):
        """This method is meant to open a json file and load its content in a python dictionary"""

        if targz_fd is None:
            with open(file, encoding="utf8") as f:
                result = load(f)

        else:
            cur_tar_file = targz_fd.extractfile(file)
            json_str = cur_tar_file.read()

            # In Python 3.5 it seems that, for some reason, the extractfile method returns an
            # object 'bytes' that cannot be managed by the function 'load' in the json package.
            # Thus, to avoid issues, in case an object having type 'bytes' is return, it is
            # transformed as a string before passing it to the function 'loads'. Please note
            # that Python 3.9 does not show this behaviour, and it works correctly without
            # any transformation.
            if type(json_str) is bytes:
                json_str = json_str.decode("utf-8")

            result = loads(json_str)

        return result

    def get_id_manager(self, schema, id_man_dict):
        """Given as input the string of a schema (e.g.:'pmid') and a dictionary mapping strings of
        the schemas to their id managers, the method returns the correct id manager. Note that each
        instance of the Preprocessing class needs its own instances of the id managers, in order to
        avoid conflicts while validating data"""
        id_man = id_man_dict.get(schema)
        return id_man

    @abstractmethod
    def split_input(self):
        """ This is the method which orchestrates the tasks performed by all the
        other methods, in order to split the input dump in lighter files, filtering
        out the entities which are not involved in citations, removing information
        which is not relevant for opencitations purposes, and validating identifiers
        """
        pass

    @abstractmethod
    def to_validated_id_list(self, id_dict_list, process_type):
        """this method takes in input a list of id dictionaries and returns a list of valid and existent ids with
        prefixes, or an updated version of the id_dict_list in input, where the identifiers in each dictionary of the
        list are validated.
        For each id, a first validation try is made by checking its presence in the META db. If the id is not in the
        META db yet, a second attempt is made by using the specific id-schema API.
        The input parameter are a list of dictionaries (containing identifiers and possibly some extra information that
        makes clear which is the identifier schema) and a string defining the type of validation process to be
        performed, in order to allow the implementation of customized solutions. For example, depending on the type of
        information, it may be necessary to keep the validated identifier paired with other data (e.g.: the case of an
        orcid and the name of an author, in which we need to return a list of dictionaries), while in other cases we
        just need a list of validated ids as output (e.g.: the case of the identifiers of the cited publications)."""
        return []

    @abstractmethod
    def splitted_to_file(self, cur_n, data, type):
        """This method takes in input an integer number which represent the number of
        entities already processed (cur_n), a list of processed entities (data) and the
        string of a filename extension (type, i.e.: '.csv'). It performs two tasks: it
        checks if the current number of processed entities can be divided by the target
        number of entities to store in each file, defined as an input parameter for each
        instance of the class Preprocessing: if it is not the case, it returns the list of
        entities in input (data); otherwise, it stores the entities in the list data to
        an output file of the required type and it returns an empty list."""
        pass

