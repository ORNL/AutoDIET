import os
import json
from warnings import warn
from datafed.CommandLib import API

from .raw_data.babel import extract_metadata, generate_thumbnails
from .utils.file_utils import make_tarfile
from .cloud.cloud_provider import CloudProvider


def make_desc_with_thumbnails(tnails, cloud, verbose=False):
    """
    Uploads the specified thumbnail images to a cloud location as publicly
    visible files and then embeds the links to these thumbnails into a
    string that can serve as the description for a DataFed data record

    Parameters
    ----------
    tnails : list
        List of tuples arranged as [(title_1, path_1), (title_2, path_2)]
    cloud : microflow.CloudProvider, Optional
        Initialized instance of microflow.DBox or microflow.GDrive
    verbose : bool, optional
        Set to True to print statements for debugging purposes. Leave False
        otherwise. Default = False

    Returns
    -------
    str
        Description for Data record containing markdown for thumbnail images
    """
    if not isinstance(cloud, CloudProvider):
        warn('cloud must be either a valid GDrive or DBox instance')
        return ""
    if not tnails:
        if verbose:
            print('Did not receive any thumbnail tuples')
        return ""

    # unpack the returned tuple
    if verbose:
        print('Got following files:')
        for pair in tnails:
            print('\t{}: {}'.format(pair[0], pair[1]))

    # Next, push all of these to CloudProvider
    if verbose:
        print('Uploading images to {}'.format(cloud))

    rem_pairs = list()
    for loc_pair in tnails:
        # Make sure to add a forward slash to the file name
        link = cloud.upload_public_file(loc_pair[1])
        if verbose:
            print('Uploaded: {} - locally at: {} to: {}'
                  ''.format(loc_pair[0], loc_pair[1], link))
        rem_pairs.append([loc_pair[0], link])

    # Now construct the description string:
    desc = ""
    for pair in rem_pairs:
        desc += "![{}]({})\n\n".format(pair[0], pair[1])

    if verbose:
        print('Markdown snippet for thumbnails:\n' + desc)

    # Now delete the local images:
    if verbose:
        print('Removing local image files now that they are on the cloud')
    for pair in tnails:
        os.remove(pair[1])
    return desc


def upload_to_datafed(file_path, web_md, coll_id, link_data=True, df_api=None,
                      scratch=None, cloud=None, verbose=False):
    """
    Converts a given data file and metadata captured from the web interface
    in DataFlow into a single DataFed data record

    Parameters
    ----------
    file_path : str
        Path to raw data file
    web_md : dict
        Metadata captured from the DataFlow web interface
    coll_id : str
        ID of DataFed collection where a new data record will be created for
        this data file
    link_data : bool, optional
        Set to True to have the data record reference the data file in its
        present location. Set to False to push the data file to DataFed
    df_api : datafed.CommandLib.API, optional
        Instance of the DataFed CommandLib API
    scratch : str, optional.
            path to directory that can be used for scratch purposes such as
            storing thumbnails or other temporary needs.
            Default = same directory where raw data is located
    cloud : microflow.CloudProvider, Optional
        Initialized instance of microflow.DBox or microflow.GDrive
    verbose : bool, optional
        Set to True to print statements for debugging purposes. Leave False
        otherwise. Default = False

    Returns
    -------
    str
        ID of DataFed record for this data file
    """
    if not df_api:
        df_api = API()

    _, file_name = os.path.split(file_path)

    if os.path.isdir(file_path):
        # file name serves as the record title
        dset_name = file_name
        # Just use the web metadata:
        full_md = {"web_metadata": web_md}
    else:

        # Individual file:
        parts = file_name.split('.')
        # TODO: What if multiple files have same base name but diff extensions?
        dset_name = '.'.join(parts[:-1])
        ext = parts[-1]

        if verbose:
            print('From this file: {}, using record title: {} and found '
                  'extension: {}'.format(file_name, dset_name, ext))

        this_md = extract_metadata(file_path, scratch=scratch, verbose=verbose)

        # combine with web_md
        if web_md is None or len(web_md) == 0:
            full_md = dict()
        else:
            full_md = {"web_metadata": web_md}
        if len(this_md) > 0:
            full_md['extracted_metadata'] = this_md

    # create data record with title of file and combined md
    if verbose:
        print('Creating data record for this file in collection: ' + coll_id)
    dc_resp = df_api.dataCreate(dset_name,
                                metadata=json.dumps(full_md),
                                # Choose to provide path later on if necessary
                                external=link_data,
                                parent_id=coll_id)
    record_id = dc_resp[0].data[0].id
    if verbose:
        print('Created data record with ID: ' + record_id)

    if link_data:
        # only provide a link to the raw data.
        # if this is a directory, then we link the directory directly
        glob_path = df_api.endpointGet() + os.path.abspath(file_path)
        if verbose:
            print('Linking this data record with data file in local file '
                  'system: ' + glob_path)
        _ = df_api.dataUpdate(record_id, raw_data_file=glob_path)
    else:
        # put raw data into record
        upload_path = file_path
        if os.path.isdir(file_path):
            # Step 1 of 3: create a tar ball
            if verbose:
                print('About to compress directory to tar ball for uploading')
            if scratch and verbose:
                print('Need to copy data over to scratch before compressing. '
                      'This could take some time...')
            # TODO: What if we cannot upload from scratch space?
            # scratch on VM is not visible to Globus endpoint!
            upload_path = make_tarfile(file_path, compress=True,
                                       tar_path=scratch)
            if verbose:
                print('Compressed directory: {} to a tar ball: {}'
                      ''.format(file_path, upload_path))
            # Step 2 of 3: dataPut

        if verbose:
            print('Uploading data file into DataFed data record '
                  '(asynchronously)')
        _ = df_api.dataPut(record_id, upload_path,
                           # Need to wait until tar is uploaded before deleting
                           # the temporary tar ball
                           wait=os.path.isdir(file_path))

        if os.path.isdir(file_path):
            # Step 3 of 3: delete the tar ball:
            if verbose:
                print('Deleting tar ball')
            os.remove(upload_path)

            # No point thinking about thumbnails:
            return record_id

    # Generate description from thumbnails
    if not cloud:
        if verbose:
            print('No cloud configured for uploading thumbnail images')
        return record_id

    if verbose:
        print('Attempting to get thumbnail')

    tnail_pairs = generate_thumbnails(file_path, record_id, scratch=scratch,
                                      verbose=verbose)

    if not tnail_pairs:
        # Could not generate description
        return record_id

    desc = make_desc_with_thumbnails(tnail_pairs, cloud, verbose=verbose)

    if verbose:
        print("Updating record's description to embed thumbnails")
    # Use the record_id to update the record
    _ = df_api.dataUpdate(record_id, description=desc)

    return record_id
