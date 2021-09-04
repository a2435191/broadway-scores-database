from abc import ABC, abstractmethod
from typing import Callable, List


class AbstractFileHostInterface(ABC):
    HOST = NotImplemented

    @abstractmethod
    def get(self,
            url_or_file_id: str,
            is_url: bool = True,
            apply_func: Callable[['BytesNode'], None] = lambda _: None,
            paths_to_avoid: List[str] = [],
            target_dir: str = ''
            ) -> 'BytesNode':

        """blocking function implemented in all AbstractFileHostInterface subclasses
        to scrape data from a file hosting service.

        :param url: url to file
        :type url: str
        """
        pass
