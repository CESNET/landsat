class RenameFileExtensionsDoNotMatch(Exception):
    def __init__(self, old_file: str, new_file: str):
        super().__init__(f"Renaming error! File extensions do not match for {old_file} and {new_file}.")


class RenameFileExistsError(Exception):
    def __init__(self, new_file):
        super().__init__(f"Renaming error! New file {str(new_file)} already exists.")
