from threading import Lock
from enums import CLASS_DATA_SPLITTER
class SearchTracker:
    def __init__(self):
        self.search_id_to_neighbors_map = dict()
        self.search_id_to_results_map = dict()
        self.search_result = dict()
        self.file_tracker = dict()

    def add_nieghbor_for_search(self, search_id, neighbor_address: str):
        with Lock:
            if search_id in self.search_id_to_neighbors_map:
                self.search_id_to_neighbors_map[search_id].append(neighbor_address)
            else:
                self.search_id_to_neighbors_map[search_id] = [neighbor_address]

    def add_result_for_search(self, search_id, result_from: str, files: list):
        with Lock:
            if search_id in self.search_id_to_results_map:
                self.search_id_to_results_map[search_id].append(result_from)
                search_result[search_id] += files
            else:
                self.search_id_to_results_map[search_id] = [result_from]
                self.search_result[search_id] += files


    def is_search_result_ready(self, search_id):
        if len(self.search_id_to_neighbors_map[search_id]) == len(self.search_id_to_results_map[search_id]):
            return True
        else:
            return False

    def get_final_search_result(self, search_id, node_search_result):
        file_to_result_map = dict()
        for search_result in  self.search_result[search_id]:
            search_result.depth += 1
            if search_result.file_name in file_to_result_map:
                if search_result.depth < file_to_result_map[search_result.file_name].depth:
                    file_to_result_map[search_result.file_name] = search_result
            else:
                file_to_result_map[search_result.file_name] = search_result
        for search_result in node_search_result[search_id]:
            file_to_result_map[search_result.file_name] = search_result
        return file_to_result_map.values()

    def update_file_tracker(self, search_result_list):
        for search_result in search_result_list:
            self.file_tracker[search_result.file_name] = search_result

    def create_results_from_files(self, files_list, node_address):
        node_search_result = []
        for file_name in files_list:
            node_search_result.append(
                FileSearchResult(
                    file_name=file_name,
                    source_address=node_address,
                    depth = 0
                )
            )
        return node_search_result
    
class FileSearchResult:
    def __init__(self, file_name, source_address, depth):
        self.file_name = file_name
        self.source = source_address
        self.depth = depth 
    
    def __str__(self):
        return [self.file_name, self.source, str(self.depth)].join(CLASS_DATA_SPLITTER)
    
    @staticmethod
    def from_str(cls, str_data):
        data = str_data.split(CLASS_DATA_SPLITTER)
        return FileSearchResult(data[0], data[1], int(data[2]))