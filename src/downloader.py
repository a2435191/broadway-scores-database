import glob
import json
import os.path
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List, Optional
from urllib.parse import urlparse

import logging
import time

import pandas
import praw

from urlextract import URLExtract
from filehost_interfaces.GDrive import GoogleDriveInterface
from filehost_interfaces.GDrive import logger as GDriveLogger
from mime_types import MimeTypes

from bytes_node import BytesNode

INTERFACE_TYPES = (GoogleDriveInterface,)


class MusicalScoresDownloader:
    logger = logging.getLogger('MusicalScoresDownloader')
    DF_COLUMNS = ('id', 'url', 'timestamp',
                  'score_urls', 'filepath')
    _URL_EXTRACTOR = URLExtract()

    def __init__(
        self,
        data_folder_path: str,
        secrets_path: str = "secrets/reddit.json",
        existing_df: pandas.DataFrame = None,
        csv_path: Optional[str] = None,
        overwrite: bool = False,
        **override_kwargs
    ) -> None:
        
        self.data_path = data_folder_path
        self.csv_path = csv_path
        self.overwrite = overwrite

        if existing_df is None:
            existing_df = pandas.DataFrame(columns=self.DF_COLUMNS)
        existing_df.set_index('id', inplace=True)
        self.existing_df = existing_df

        with open(secrets_path) as f:
            kwargs = json.load(f)

        kwargs.update(override_kwargs)

        self.reddit = praw.Reddit(**kwargs)
        assert self.reddit.user.me().name == kwargs['username']

        self.subreddit = self.reddit.subreddit("MusicalScores")

    def download(self) -> None:
        # TODO: go back more than 1000
        for submission in self.subreddit.search('flair:Submission', 'new', limit=1000):
            if not self.overwrite and submission.id in self.existing_df.index:
                continue

            urls = self._URL_EXTRACTOR.find_urls(
                submission.selftext) if submission.is_self else [submission.url]

            if len(urls) == 0:
                self.logger.warning(f'No URLs found in post (id={submission.id})')

            for i, url in enumerate(urls):
                node = self.download_from_interface_url(url, os.path.join(
                    self.data_path, f'{BytesNode.clean_path(submission.title)}/link{i}'))
                if node is None:
                    self.logger.warning(f'No matching class found for {url}!')
                    continue

            self.existing_df.loc[submission.id, :] = (
                submission.permalink,
                time.time(),
                ' ; '.join(urls),
                os.path.join(self.data_path, f'{submission.title}: link n')
            )
            if self.csv_path is not None:
                self.existing_df.to_csv(self.csv_path)

    def download_from_interface_url(self, url: str, target_dir: str) -> Optional['BytesNode']:
        host = urlparse(url).hostname
        try:
            interface = next((i for i in INTERFACE_TYPES if i.HOST == host))
        except StopIteration:
            return None

        interface_obj = interface()
        head = interface_obj.get(
            url,
            apply_func=self._save_to_disk,
            paths_to_avoid=[],
            target_dir=target_dir
        )

        return head

    @staticmethod
    def _save_to_disk(node: 'BytesNode') -> None:
        if node.mime_type == MimeTypes.PDF:
            ext = '.pdf'
        elif node.mime_type == MimeTypes.ZIP:
            ext = '.zip'
        else:
            return
        

        dirname = os.path.dirname(node.file_path)
        try:
            os.makedirs(dirname)
        except FileExistsError:
            pass

        outfilepath = node.file_path if node.file_path.endswith(
            ext) else node.file_path + ext
        with open(outfilepath, 'wb+') as f:
            f.write(node.bytes_obj)


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.WARNING)
    GDriveLogger.setLevel(logging.INFO)

    from anytree import RenderTree
    from anytree.render import AsciiStyle

    SMALL_TEST_URL = 'https://drive.google.com/drive/folders/19FHcDNj7pB59UEC2Qr-1esLnpwB7sBgl?usp=sharing'
    BIG_TEST_URL = 'https://drive.google.com/drive/folders/1feIVeC-paXfxBgIK_xDy2G7kSLIsyrXe'

    api = MusicalScoresDownloader(
        'data/scores', 
        existing_df = pandas.read_csv('data/data.csv'), 
        csv_path='data/data.csv', 
        overwrite=False
    )
    api.download()
