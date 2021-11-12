import sys
import os
from warnings import warn
import json
from .dbox import DBox
from .gdrive import GDrive


def setup_tnail_cloud(cloud_config):
    """
    Sets up an instance of either the microflow.DBox or microflow.GDrive
    using the information provided in a JSON file

    Parameters
    ----------
    cloud_config : str
        Path to JSON file containing information necessary to set up an
        instance of microflow.DBox or microflow.GDrive

    Returns
    -------
    microflow.DBox or microflow.GDrive
    """
    if not os.path.exists(cloud_config):
        raise FileNotFoundError('CloudProvider configuration path: {} does not'
                                ' exist'.format(cloud_config))

    with open(cloud_config) as file_handle:
        config = json.load(file_handle)

    # First figure out the cloud provider
    provider = config.pop("provider", None)
    if not isinstance(provider, str):
        return
    provider = provider.lower()

    if provider == "google_drive":
        token_path = config.pop("token_path", None)
        dest_dir_id = config.pop("drive_directory", None)
        if token_path:
            try:
                return GDrive(token_path, dest_dir_id=dest_dir_id)
            except Exception as _:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                warn("Could not set up Google Drive\n"
                     "{}: '{}'\nFrom:{}".format(exc_type, fname,
                                                exc_tb.tb_lineno))
            return None
    elif provider == "dropbox":
        acc_token = config.pop("access_token", None)
        if acc_token:
            try:
                return DBox(acc_token)
            except Exception as _:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                warn("Could not set up Google Drive\n"
                     "{}: '{}'\nFrom:{}".format(exc_type, fname,
                                                exc_tb.tb_lineno))
    else:
        raise NotImplementedError("Do not know how to handle: "
                                  "{}".format(provider))
