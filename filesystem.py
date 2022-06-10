import os  

class FileSystem:
    def __init__(self, folder_address):
        pass

    def search_for_file(self, searched_name: str):
        files = []
        try:
            with os.scandir(self.folder_address) as entries:
                for entry in entries:
                    if os.path.isfile(os.path.join(self.folder_address, entry.name)):
                        if searched_name.lower() in entry.name.lower():
                            files.append(entry.name)
        except OSError:
            print("ERROR: folder does not exist")
        return files  

    def get_file_content(self, file_name: str) -> str:
        file_path = os.path.join(self.folder_address, file_name)
        f = open(file_path, 'rb')
        file_content = f.read()
        f.close()
        return file_content

    def add_new_file(self,file_content, file_name) -> None:
        with open(file = file_name,mode = 'w') as new_file:
            new_file.write(file_content)
            new_file.close()

