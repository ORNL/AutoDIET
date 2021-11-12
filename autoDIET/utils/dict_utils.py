from math import isinf, isnan
import h5py
import numpy as np


def pretty_print_dict(my_dict, level=0):
    """
    Prints nested dictionaries in a nested manner

    Parameters
    ----------
    my_dict : dict
        Dictionary to print to stdout
    level : int
        Depth of my_dict from original root. Do not specify.

    Returns
    -------

    """
    for key, val in my_dict.items():
        if isinstance(val, dict):
            print('\t' * level + key + " :")
            pretty_print_dict(val, level=level + 1)
        else:
            print('\t' * level + "{} : {}".format(key, val))


def parse_dict(raw_dict, ignore_keys=[]):
    """
    Parses the values in the dictionary as booleans, ints, and floats as
    appropriate

    Parameters
    ----------
    raw_dict : dict
        Flat dictionary whose values are mainly strings
    ignore_keys : list, optional
        Keys in the dictionary to remove

    Returns
    -------
    dict
        Flat dictionary with values of expected dtypes
    """

    def __parse_str(mystery):
        if not isinstance(mystery, str):
            return mystery
        if mystery.lower() == 'true':
            return True
        if mystery.lower() == 'false':
            return False
        # try to convert to number
        try:
            mystery = float(mystery)
            if mystery % 1 == 0:
                mystery = int(mystery)
                return mystery
        except ValueError:
            return mystery

    if not ignore_keys:
        ignore_keys = list()
    else:
        if isinstance(ignore_keys, str):
            ignore_keys = [ignore_keys]
        elif not isinstance(ignore_keys, (list, tuple)):
            raise TypeError('ignore_keys should be a list of strings')
    clean_dict = dict()
    for key, val in raw_dict.items():
        if key in ignore_keys:
            continue
        val = __parse_str(val)
        if isinstance(val, (tuple, list)):
            val = [__parse_str(item) for item in val]
        elif isinstance(val, dict):
            val = parse_dict(val, ignore_keys=ignore_keys)
        clean_dict[key] = val
    return clean_dict


def clean_attributes(metadata):
    """
    Back-converts numpy data types to base python data types.
    This is a necessary step before exporting metadata to json

    Parameters
    ----------
    metadata : dict
        Flat or nested dictionary

    Returns
    -------
    metadata: dict
        Dictionary whose values are base python objects
    """
    attrs_to_delete = []
    for key, val in metadata.items():
        if not val:
            # JSON does not like None
            metadata[key] = "None"
        elif isinstance(val, dict):
            metadata[key] = clean_attributes(val)
        elif type(val) in [np.uint16, np.uint8, np.uint, np.uint32, np.int,
                           np.int16, np.int32, np.int64]:
            metadata[key] = int(val)
        elif isinstance(val, float) and (isinf(val) or isnan(val)):
            metadata[key] = "NaN"
        elif type(val) in [np.float, np.float16, np.float32, np.float64]:
            metadata[key] = float(val)
        elif type(val) in [np.bool, np.bool_]:
            metadata[key] = bool(val)
        elif isinstance(val, np.ndarray):
            metadata[key] = val.tolist()
        elif isinstance(val, h5py.Reference):
            attrs_to_delete.append(key)
        elif isinstance(val, bytes):
            metadata[key] = val.decode("utf-8")

    for key in attrs_to_delete:
        del metadata[key]

    return metadata
