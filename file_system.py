from dataclasses import dataclass
import os
from typing import List


@dataclass
class FileSystemSearchResult:
    name: str
    size: int


class FileSystem:
    def __init__(self, folder_address):
        self.folder_address = folder_address

    def search_for_file(self, searched_name: str) -> List[FileSystemSearchResult]:
        files = []
        try:
            with os.scandir(self.folder_address) as entries:
                for entry in entries:
                    if os.path.isfile(os.path.join(self.folder_address, entry.name)):
                        if searched_name.lower() in entry.name.lower():
                            files.append(
                                FileSystemSearchResult(
                                    name=entry.name,
                                    size=entry.stat().st_size,
                                )
                            )
        except OSError:
            print("ERROR: folder does not exist")
        return files

    def get_file_content(self, file_name: str) -> str:
        file_path = os.path.join(self.folder_address, file_name)
        with open(file_path, 'r') as f:
            return f.read()

    def add_new_file(self, file_content, file_name) -> None:
        with open(file=os.path.join(self.folder_address, file_name), mode='w') as new_file:
            new_file.write(file_content)
