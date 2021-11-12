import os
import dropbox
from .cloud_provider import CloudProvider


class DBox(CloudProvider):

    def __init__(self, access_token):
        """
        Initializes class

        Parameters
        ----------
        access_token : str
            Dropbox access token
        """
        if not isinstance(access_token, str):
            raise TypeError('Access token must be a string')
        access_token = access_token.strip()
        if len(access_token) < 1:
            raise ValueError("Access token must not be empty")
        self.__service__ = dropbox.Dropbox(access_token)

    def upload_public_file(self, local_path, dest_path=None):
        """
        Upload the provided file to the specified Dropbox location
        and share a publicly visible_file

        Parameters
        ----------
        local_path : str
            Path to local file that needs to be uploaded
        dest_path : str, Optional
            Path in Dropbox to upload to. Note that this is likely within
            the context (directory) where permissions are valid

        Returns
        -------
        str :
            Publicly visible address where the file can be accessed
        """
        if not dest_path:
            _, file_name = os.path.split(local_path)
            dest_path = '/' + file_name

        with open(local_path, "rb") as file_handle:
            _ = self.__service__.files_upload(file_handle.read(), dest_path)
        share_metadata = self.__service__.sharing_create_shared_link_with_settings(dest_path,
                                                                                   settings=dropbox.sharing.SharedLinkSettings())
        # The shareable link takes one to dropbox instead of the file itself:
        embed_link = share_metadata.url.replace("www.dropbox",
                                                "dl.dropboxusercontent")
        return embed_link
