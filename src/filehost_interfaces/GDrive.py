import io
import json
import os
from typing import Optional, Dict, Callable, List
from urllib.parse import urlparse

import shutil

import requests

from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError

from .base import AbstractFileHostInterface
from bytes_node import BytesNode
from mime_types import MimeTypes

import logging

logger = logging.getLogger(__name__)

class GoogleDriveInterface(AbstractFileHostInterface):
    HOST = 'drive.google.com'
    URL = 'https://drive.google.com/uc'

    _SCOPES = ('https://www.googleapis.com/auth/drive.readonly',)

    PATH_TO_USER_TOKENS = 'secrets/drive_user_token.json'
    PATH_TO_CREDENTIALS = 'secrets/drive_app_credentials.json'

    

    def get(self, url_or_file_id: str, is_url: bool = True, apply_func: Callable[[BytesNode], None] = lambda _: None, paths_to_avoid: List[str] = [], target_dir: str = '') -> BytesNode:
        file_id = self._get_file_id(
            url_or_file_id) if is_url else url_or_file_id

        

        def recurse(file_id: str, parent_node: BytesNode) -> BytesNode:
            logger.debug(f'(id={file_id}) beginning download')

            metadata_requester = self.api.files().get(fileId=file_id)
            try:
                metadata = metadata_requester.execute()
            except HttpError as e:
                logger.warning('exception', exc_info=e)
                return parent_node

            mime_type = metadata['mimeType']

            metadata_node = BytesNode(b'', metadata, parent=parent_node, base_dir=target_dir)

            for path in paths_to_avoid:
                logger.debug(f"checking for path equality: {path} ?= {metadata_node.file_path}")
                try:
                    is_samefile = os.path.samefile(path, metadata_node.file_path)
                except FileNotFoundError:
                    continue
                if is_samefile:
                    logger.info(
                        f"refusing to overwrite {os.path.abspath(path)}")
                    return parent_node

            if mime_type == MimeTypes.PDF or mime_type == MimeTypes.ZIP:

                data_requester = self.api.files().get_media(fileId=file_id)
                fh = io.BytesIO()

                downloader = MediaIoBaseDownload(fh, data_requester)
                done = False
                while done is False:
                    _, done = downloader.next_chunk()

                fh.seek(0)
                content = fh.read()

                if len(content) == 0:
                    logger.warn(f'(id={file_id}) empty content')

                node = BytesNode(content, metadata=metadata, parent=parent_node, base_dir=target_dir)
                apply_func(node)

                logger.debug(f'(id={file_id}) got PDF: {metadata}')

            elif mime_type == MimeTypes.FOLDER:
                folder_search_requester = self.api.files().list(
                    q=f"'{file_id}' in parents")
                folder_metadata = folder_search_requester.execute()
                files_in_folder = folder_metadata['files']

                logger.debug(
                    f'(id={file_id}) got folder metadata: {folder_metadata}')

                folder_node = BytesNode(b'', metadata=metadata, parent=parent_node, base_dir=target_dir)
                
                for file_data in files_in_folder:
                    id_ = file_data['id']
                    folder_node = recurse(id_, folder_node)

                folder_node.parent = parent_node
                apply_func(folder_node)
            else:
                logger.warning(
                    f'(id={file_id}) other mimetype: {mime_type} passed in')
                #raise RuntimeError(f"Mime type {mime_type} not supported. Must be one of MimeTypes.")

            metadata_node.parent = None

            logger.info(
                f'(id={file_id}) finished downloading file'
            )

            return parent_node

        out = recurse(file_id, BytesNode(b'', {}, name='__head__', base_dir=target_dir))
        logger.info(
            f'(id={file_id}) finished downloading tree. Total nodes: {len(out.descendants)}'
        )

        return out

    def __init__(self):
        self.auth()
        self.api = build(
            'drive',
            'v3',
            credentials=Credentials.from_authorized_user_file(
                self.PATH_TO_USER_TOKENS, self._SCOPES
            )
        )

    @classmethod
    def auth(cls) -> None:
        """Authenticate a user using app credentials stored locally.
        """

        # adapted from
        # https://developers.google.com/drive/api/v3/quickstart/python

        cls.PATH_TO_CREDENTIALS = 'secrets/drive_app_credentials.json'

        creds = None
        if os.path.exists(cls.PATH_TO_USER_TOKENS):
            creds = Credentials.from_authorized_user_file(
                cls.PATH_TO_USER_TOKENS, cls._SCOPES)
        if not creds or not creds.valid:
            refresh = creds and creds.expired and creds.refresh_token
            try:
                creds.refresh(Request())
            except RefreshError:
                flow = InstalledAppFlow.from_client_secrets_file(
                    cls.PATH_TO_CREDENTIALS, cls._SCOPES)
                creds = flow.run_local_server(port=0)
            
            # Save the credentials for the next run
            with open(cls.PATH_TO_USER_TOKENS, 'w+') as f:
                f.write(creds.to_json())

    @classmethod
    def _get_file_id(cls, url: str) -> str:
        """Extract a file ID from a Google Drive URL.
        :param url:
        :type url: str
        :return: longest single path in url
        :rtype: str
        """

        "https://drive.google.com/file/d/1kUyRz7t0PsIBcT8ppM__CJcLFjetPJRE/view?usp=sharing"
        path = urlparse(url).path
        file_id = max(path.split('/'), key=len)
        return file_id
