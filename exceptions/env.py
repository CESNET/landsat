class EnvError(Exception):
    def __init__(self, message="Configuration error!"):
        self.message = message
        super().__init__(self.message)


class UnexpectedDataset(EnvError):
    def __init__(self, message="Unexpected dataset(s) selected!", selected_datasets: list = None):
        if selected_datasets:
            message = message + f" Dateset array: {selected_datasets}"
        self.message = message
        super().__init__(self.message)
