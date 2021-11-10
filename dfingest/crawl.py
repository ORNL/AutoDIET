import os
import json
from warnings import warn
from datafed.CommandLib import API
from .utils.datafed_utils import list_all_items_in_coll
from .utils.dict_utils import pretty_print_dict
from .cloud.cloud_provider import CloudProvider
from .cloud.cloud_spawn import setup_tnail_cloud
from .ingest import upload_to_datafed


def process_posix_coll(dir_path, coll_id, df_api=None, link_data=True,
                       scratch=None, cloud=None, verbose=False):
    """
    Ingests the content of a single dataset's worth of files (uploaded via
    DataFlow) into DataFed.
    Metadata in the "metadata.json" file will be used for all other files
    present in the directory.

    Parameters
    ----------
    dir_path : str
        Path in local file system containing the raw data and metadata for a
        single dataset.
    coll_id : str
        ID for parent DataFed collection within which this "dataset" from
        DataFlow will be ingested
    df_api : datafed.CommandLib.API instance, Optional
        Instance of the DataFed CommandLib API
    link_data : bool, optional
        Set to True to have the data record reference the data file in its
        present location. Set to False to push the data file to DataFed
    scratch : str, optional.
            path to directory that can be used for scratch purposes such as
            storing thumbnails or other temporary needs.
            Default = same directory where raw data is located
    cloud : microflow.CloudProvider, Optional
        Initialized instance of microflow.DBox or microflow.GDrive
    verbose : bool, optional
        Set to True to print statements for debugging purposes. Leave False
        otherwise. Default = False
    """
    if not df_api:
        df_api = API()

    json_path = None
    for file_name in os.listdir(dir_path):
        if file_name.lower() == 'metadata.json':
            json_path = os.path.join(dir_path, file_name)
            if verbose:
                print('\tFound JSON: ' + json_path)
            try:
                with open(json_path, mode='r') as json_handle:
                    web_md = json.load(json_handle)
            except json.JSONDecodeError:
                warn('Could not decode metadata. Probably not in JSON form')
                web_md = dict()
    if not json_path:
        warn('metadata.json not found in {}'.format(dir_path))
        # Use an empty dictionary.
        web_md = dict()

    if verbose:
        print('\tMetadata from DataFlow web interface:')
        pretty_print_dict(web_md)

    existing_recs = list_all_items_in_coll(coll_id, mode="d/")
    if verbose:
        print('\tFound these data records already on DataFed:')
        print('\t' + str(existing_recs))

    for file_name in os.listdir(dir_path):
        if file_name == 'metadata.json' or file_name.startswith('.'):
            if verbose:
                print('Not creating DataFed record for file: ' + file_name)
            continue

        file_path = os.path.join(dir_path, file_name)
        if os.path.isdir(file_path):
            item_name = file_name
        else:
            # remove extension from file name and use as title
            item_name = '.'.join(file_name.split('.')[:-1])

        if item_name in existing_recs.keys():
            print(item_name + ' already present in collection')
            continue

        if verbose:
            print('Need to create record for: ' + file_name)
        upload_to_datafed(file_path, web_md, coll_id, df_api=df_api,
                          link_data=link_data, scratch=scratch, cloud=cloud,
                          verbose=verbose)
        if verbose:
            print('\n' * 5)


def sync_posix_dfed(local_dir, dfed_coll, max_depth=1, df_api=None,
                    link_data=True, scratch=None, cloud=None, verbose=False):
    """
    Recursively mines the provided directory path in the local file system and
    ingests any data not already in DataFed into DataFed.
    Metadata in the "metadata.json" file will be used for all other files
    present in the directory.

    Parameters
    ----------
    local_dir : str
        Root directory path in local file system that contains data uploaded
        using (an older version of) DataFlow for a specific instrument
    dfed_coll : str
        ID for corresponding DataFed collection into which data records and
        collections will be created to mirror the organization of the
        file-system
    max_depth : int, optional.
        Number of levels into the file system to go into to get to
        individual dataset folders (in DataFlow speak). In other words,
        this is the number of intermediate directories between the provided
        local_dir and the individual dataset directories
        Default is set to how DataFlow is configured as of 9/28/2021
    df_api : datafed.CommandLib.API instance
        Instance of the DataFed CommandLib API
    link_data : bool, optional
        Set to True to have the data record reference the data file in its
        present location. Set to False to push the data file to DataFed
    scratch : str, optional.
            path to directory that can be used for scratch purposes such as
            storing thumbnails or other temporary needs.
            Default = same directory where raw data is located
    cloud : str or microflow.CloudProvider, Optional
        Path to JSON file containing necessary information for cloud hosting
        of thumbnails
        OR Initialized instance of microflow.DBox or microflow.GDrive
    verbose : bool, optional
        Set to True to print statements for debugging purposes. Leave False
        otherwise. Default = False
        """
    if not df_api:
        df_api = API()

    if cloud:
        if not isinstance(cloud, CloudProvider):
            cloud = setup_tnail_cloud(cloud)

    if verbose:
        print("Syncing {} with {}. Allowed to go {} levels deep"
              "".format(local_dir, dfed_coll, max_depth))

    existing_child_colls = list_all_items_in_coll(dfed_coll, mode="c/",
                                                  df_api=df_api)

    if verbose:
        print('Existing collections in {}:'.format(dfed_coll))
        print(existing_child_colls)

    for dir_name in os.listdir(local_dir):
        this_child_path = os.path.join(local_dir, dir_name)

        if this_child_path == scratch:
            # Ignore scratch within file system
            continue

        if not os.path.isdir(this_child_path):
            # TODO: What should we do about files in higher levels
            # Ignore files at this level
            continue
        if dir_name not in existing_child_colls.keys():
            if verbose:
                print('Creating collection for sub-dir: ' + dir_name)
            cc_resp = df_api.collectionCreate(dir_name, parent_id=dfed_coll)
            existing_child_colls[dir_name] = cc_resp[0].coll[0].id
        else:
            if verbose:
                print('Already have a collection for dir: {} ID: {}'
                      ''.format(dir_name, existing_child_colls[dir_name]))
        if max_depth > 0:
            sync_posix_dfed(this_child_path, existing_child_colls[dir_name],
                            max_depth=max_depth-1, link_data=link_data,
                            df_api=df_api, scratch=scratch, cloud=cloud,
                            verbose=verbose)
        else:
            process_posix_coll(this_child_path, existing_child_colls[dir_name],
                               link_data=link_data, scratch=scratch,
                               df_api=df_api, cloud=cloud, verbose=verbose)


if __name__ == "__main__":
    dflow_root = os.path.join(os.environ['HOME'],
                              "dataflow/untitled-instrument/")
    dfed_root = "c/p_mat004_root"

    sync_posix_dfed(dflow_root, dfed_root, cloud='./dropbox.json',
                    max_depth=1, verbose=True)
