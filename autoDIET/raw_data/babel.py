from ..utils.dict_utils import clean_attributes, pretty_print_dict
from ..raw_data.parser import Parser
from ..raw_data.images import Images

# Domain scientists to add more cases here.
parsers = [Images, Parser]


def get_parser(file_path, scratch=None, verbose=False):
    """
    Returns a Parser class capable of reading the provided file

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

    Returns
    -------
    microfow.raw_data.parser.Parser
        Child class of the Parser class
    """
    for this_class in parsers:
        try:
            reader = this_class(file_path, scratch=scratch, verbose=verbose)
            return reader
        except Exception as _:
            pass
    return None


def extract_metadata(file_path, scratch=None, verbose=False):
    """
    Extracts metadata present within the provided data file.
    This is the function that domain scientists can add to.

    Parameters
    ----------
    file_path : str
        Path to raw data file
    scratch : str, optional.
            path to directory that can be used for scratch purposes such as
            storing thumbnails or other temporary needs.
            Default = same directory where raw data is located
    verbose : bool, optional
        Set to True to print statements for debugging purposes. Leave False
        otherwise. Default = False

    Returns
    -------
    dict
        Dictionary with domain-specific metadata
    """
    this_parser = get_parser(file_path, scratch=scratch, verbose=verbose)
    if not this_parser:
        # we don't have any means for extracting MD.
        if verbose:
            print('No Parser to extract metadata from file: ' + file_path)
        return dict()

    assert isinstance(this_parser, Parser)
    sci_md = this_parser.get_metadata()
    # Put the burden on cleaning for JSON here rather than on the Parsers
    sci_md = clean_attributes(sci_md)

    if verbose:
        print('Metadata extracted from data file: ' + file_path)
        pretty_print_dict(sci_md)
    return sci_md


def generate_thumbnails(file_path, record_id, scratch=None, verbose=False):
    """
    Generates the description string for any given data file.
    This is the function that domain scientists can add to.

    Parameters
    ----------
    file_path : str
        Path to raw data file
    record_id : str
        DataFed Data record ID formatted as "d/12345678"
    scratch : str, optional.
            path to directory that can be used for scratch purposes such as
            storing thumbnails or other temporary needs.
            Default = same directory where raw data is located
    verbose : bool, optional
        Set to True to print statements for debugging purposes. Leave False
        otherwise. Default = False

    Returns
    -------
    list:
        List of tuples of size 2 where each tuple is a pair of
        alternate text for image and path to thumbnail image
    """
    this_parser = get_parser(file_path, scratch=scratch, verbose=verbose)
    if not this_parser:
        # we don't have any means for extracting thumbnails
        if verbose:
            print('No Parser to extracting thumbnails from: ' + file_path)
        return None

    # Remove the "d/" - messes with file names.
    num_id = record_id.split('/')[-1]

    assert isinstance(this_parser, Parser)
    tnail_pairs = this_parser.get_thumbnails(num_id)

    return tnail_pairs
