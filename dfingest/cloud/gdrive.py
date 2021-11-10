import os.path
import mimetypes
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

from .cloud_provider import CloudProvider


class GDrive(CloudProvider):

    def __init__(self, token_json_path, dest_dir_id=None):
        """
        Gets an authenticated instance of the Google Drive API

        Parameters
        ----------
        token_json_path : str
            Path to the JSON file with the renewal tokens etc.
        dest_dir_id : str, Optional.
            ID of the Google drive folder that is already publicly visible
            If not provided, files will be placed in root of Google Drive

        Returns
        -------
        googleapiclient.discovery.Resource
        """
        SCOPES = ['https://www.googleapis.com/auth/drive']
        creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists(token_json_path):
            creds = Credentials.from_authorized_user_file(token_json_path,
                                                          SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(token_json_path, 'w') as token:
                token.write(creds.to_json())

        self.__service__ = build('drive', 'v3', credentials=creds)
        # TODO: Don't require folder itself to be publicly visible
        self.dest_dir_id = dest_dir_id

    def upload_public_file(self, src_path, dest_name=None):
        """

        Parameters
        ----------
        src_path : str
            Path of local file to be uploaded
        dest_name : str, Optional
            Alternate name of file with appropriate extension
            If not provided, the source file name will be used.

        Returns
        -------
        str :
            Publicly visible link where this file can be accessed

        Notes
        -----
        for embed > https://drive.google.com/file/d/file's ID/preview
        for direct link > https://drive.google.com/uc?export=view&id=file's ID
        for share > https://drive.google.com/file/d/file's ID/view?usp=sharing
        """
        if not isinstance(self.dest_dir_id, str):
            raise TypeError('dest_dir_id should be a string. Object was of type: '
                            '{}'.format(type(self.dest_dir_id)))
        src_dir, src_name = os.path.split(src_path)
        if not dest_name:
            dest_name = src_name
        else:
            if not isinstance(self.dest_dir_id, str):
                raise TypeError('dest_name should be a string. Object was of type:'
                                ' {}'.format(type(dest_name)))
            # TODO: ensure that extensions match though
        file_metadata = {"name": dest_name,
                         "parents": [self.dest_dir_id]}
        mime_type, _ = mimetypes.guess_type(src_path)
        media = MediaFileUpload(src_path, mimetype=mime_type)
        resp = self.__service__.files().create(
                                    body=file_metadata,
                                    media_body=media,
                                    fields="id",
                                    ).execute()

        # TODO: Implement link generation for individual file

        return "https://drive.google.com/uc?export=view&id={}".format(resp['id'])

    def create_folder(self, title, parents=None):
        """
        Creates a folder within the specified parent

        Parameters
        ----------
        title : str
            Title of new folder
        parents : str, Optional


        Returns
        -------
        str :
            ID of created folder
        """
        if not isinstance(title, str):
            raise TypeError('title must be a string')
        if not parents:
            parents = []
        elif isinstance(parents, str):
            parents = [parents]
        elif not isinstance(parents, list):
            raise TypeError('parents should either be a str or a list of str')
        file_metadata = {
            'name': title,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': parents
        }
        file = self.__service__.files().create(body=file_metadata,
                                               fields='id').execute()
        return file.get('id')
