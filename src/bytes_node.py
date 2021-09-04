import os.path
from typing import Optional, Dict
from anytree import NodeMixin

class BytesNode(NodeMixin):
    def __init__(self,
                 bytes_obj: bytes,
                 metadata: Dict,
                 name: Optional[str] = None,
                 parent: Optional['BytesNode'] = None,
                 base_dir: str = ""
                 ):

        super().__init__()
        self.bytes_obj = bytes_obj
        self.metadata = metadata
        self.mime_type = self.metadata.get('mimeType')

        if parent:
            self.parent = parent
        if name is None:
            self.name = self.metadata.get('name')
        else:
            self.name = name

        self.file_path = os.path.join(
            base_dir, *[self.clean_path(i.name) for i in self.path])

    def __repr__(self):
        return f"BytesNode(bytes_obj = <bytes object of length {len(self.bytes_obj)}>, metadata = {self.metadata}, name = {self.name}), file_path = {self.file_path}"

    def __str__(self):
        return self.name

    @staticmethod
    def clean_path(path: str) -> str:
        if path == '__head__':
            return ''
        return path.replace('/', "\\")
