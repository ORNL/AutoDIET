import sys
import os
from warnings import warn
import wget
import tika
from tika import parser

from microflow.utils.file_utils import validate_scratch_dir
from microflow.utils.dict_utils import parse_dict


def _get_local_kita_jar(root_dir, version=None, verbose=False):
    """
    Get's a local copy of the Apache Kita jar file for faster lookups

    Parameters
    ----------
    root_dir : str
        Location where to look for existing copies of Apache Kita jar file
        This should be a location where this code has write access
    version : str, Optional
        Apache Kita version. Default = "1.27"
    verbose : bool, optional
            Set to True to print statements for debugging purposes. Leave False
            otherwise. Default = False

    Returns
    -------
    str
        Path to local Apache Kita jar file
    """
    if not version:
        version = '1.27'
    else:
        if not isinstance(version, str):
            raise TypeError('version must be a string like 1.27')

    target_jar = 'tika-server-{}.jar'.format(version)

    if verbose:
        print("Looking for '" + target_jar + "' in: " + root_dir)

    for item in os.listdir(root_dir):
        if os.path.isdir(os.path.join(root_dir, item)):
            continue
        if item == target_jar:
            if verbose:
                print('Found existing jar file: ' + os.path.join(root_dir, item))
            return os.path.join(root_dir, item)
    # At this point, jar file was not found locally
    # Download from Apache
    if verbose:
        print('Existing jar file not found. Downloading from Apache')
    return wget.download('https://dlcdn.apache.org/tika/{}/tika-server-{}.jar'
                         ''.format(version, version),
                         out=os.path.join(root_dir, target_jar))


class Parser(object):

    def __init__(self, file_path, *args, scratch=None, verbose=False,
                 **kwargs):
        """
        Constructor for a Parser

        Parameters
        ----------
        file_path : str
            Path to a data file or directory containing data associated
            with a single dataset
        scratch : str, optional.
            path to directory that can be used for scratch purposes such as
            storing thumbnails or other temporary needs.
            Default = same directory where raw data is located
        verbose : bool, optional
            Set to True to print statements for debugging purposes. Leave False
            otherwise. Default = False
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError('File for this dataset does not exist: '
                                    '{}'.format(file_path))
        self.file_path = file_path
        self.verbose = verbose
        self.scratch = validate_scratch_dir(scratch, verbose=verbose)
        if not self.scratch and verbose:
            print('No scratch provided. Will attempt to write to same '
                  'directory as data')

    def get_metadata(self):
        """
        Gets metadata from this dataset as a dictionary.
        By default, Apache Kita will be used to get metadata

        Returns
        -------
        dict
            Dictionary with metadata
        """
        parent_dir, _ = os.path.split(self.file_path)

        if not self.scratch:  # and not os.access(parent_dir, os.W_OK):
            warn('Nowhere to place Tika jar file. Using Tika in online mode')
            # Rely on directly talking to the mothership
        else:
            # TODO: Resort to downloading only if online mode fails.
            os.environ['TIKA_SERVER_JAR'] = _get_local_kita_jar(self.scratch, verbose=self.verbose)

        try:
            tika.initVM()
            parsed = parser.from_file(self.file_path)
        except Exception as _:
            # If anything goes wrong, just tell the user it was not possible
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            warn("{}: '{}'\nFrom:{}".format(exc_type, fname, exc_tb.tb_lineno))
            return dict()

        if not parsed:
            if self.verbose:
                print('Tika returned metadata as None')
            return dict()
        if len(parsed) == 0:
            if self.verbose:
                print('Tika returned empty metadata')
            return dict()

        ignore_keys = ['X-Parsed-By', 'X-TIKA:embedded_depth', 'Content-Type',
                       'X-TIKA:parse_time_millis', 'resourceName']

        return parse_dict(parsed['metadata'], ignore_keys=ignore_keys)

    def get_thumbnails(self, base_name, *args, **kwargs):
        """
        Generates thumbnail image file(s) based on the raw data.

        Parameters
        ----------
        base_name : str
            Prefix for the thumbnail image files. Use DataFed record ID here.

        Returns
        -------
        list
            List of tuples arranged as [(title_1, path_1), (title_2, path_2)]
        """
        return None
