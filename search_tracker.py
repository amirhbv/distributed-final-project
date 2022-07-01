from dataclasses import dataclass
from threading import Lock
from typing import List

from enums import CLASS_DATA_SPLITTER
from file_system import FileSystemSearchResult


class SearchTracker:
    def __init__(self):
        self.search_id_to_neighbors_map = dict()
        self.search_id_to_results_map = dict()
        self.search_result = dict()
        self.file_tracker = dict()
        self.add_neighbor_lock = Lock()
        self.add_result_lock = Lock()

    def get_file_search_result_by_file_name(self, file_name):
        return self.file_tracker[file_name]

    def add_nieghbor_for_search(self, search_id, neighbor_address: str):
        with self.add_neighbor_lock:
            if search_id in self.search_id_to_neighbors_map:
                self.search_id_to_neighbors_map[search_id].append(
                    neighbor_address)
            else:
                self.search_id_to_neighbors_map[search_id] = [neighbor_address]

    def add_result_for_search(self, search_id, result_from: str, files: list):
        with self.add_result_lock:
            if search_id in self.search_id_to_results_map:
                self.search_id_to_results_map[search_id].append(result_from)
                self.search_result[search_id] += files
            else:
                self.search_id_to_results_map[search_id] = [result_from]
                self.search_result[search_id] = files

    def is_search_result_ready(self, search_id):
        if search_id in self.search_id_to_results_map:
            return len(self.search_id_to_neighbors_map[search_id]) == len(self.search_id_to_results_map[search_id])
        else:
            return False

    def get_final_search_result(self, search_id, node_search_result):
        file_to_result_map = dict()
        for search_result in self.search_result[search_id]:
            search_result.depth += 1
            if search_result.file_name in file_to_result_map:
                if search_result.depth < file_to_result_map[search_result.file_name].depth:
                    file_to_result_map[search_result.file_name] = search_result
            else:
                file_to_result_map[search_result.file_name] = search_result
        for search_result in node_search_result:
            file_to_result_map[search_result.file_name] = search_result

        self.update_file_tracker(file_to_result_map.values())
        return list(file_to_result_map.values())

    def update_file_tracker(self, search_result_list):
        for search_result in search_result_list:
            self.file_tracker[search_result.file_name] = search_result
        print('file_tracker', self.file_tracker)

    def create_results_from_files(self, files: List[FileSystemSearchResult], node_address):
        node_search_result = []
        for file_system_search_result in files:
            node_search_result.append(
                FileSearchResult(
                    file_name=file_system_search_result.name,
                    file_size=file_system_search_result.size,
                    source=node_address,
                    depth=0
                )
            )
        return node_search_result


@dataclass
class FileSearchResult:
    file_name: str
    file_size: int
    source: str
    depth: int

    def __str__(self):
        return CLASS_DATA_SPLITTER.join([self.file_name, str(self.file_size), self.source, str(self.depth)])

    @staticmethod
    def from_str(str_data):
        data = str_data.split(CLASS_DATA_SPLITTER)
        return FileSearchResult(data[0], int(data[1]), data[2], int(data[3]))
