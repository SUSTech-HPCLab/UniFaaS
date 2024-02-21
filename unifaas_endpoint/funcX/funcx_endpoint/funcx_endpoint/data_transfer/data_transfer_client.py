from abc import abstractclassmethod, ABCMeta

class DataTransferClient(metaclass=ABCMeta):
    @abstractclassmethod
    def transfer(self, data):
        pass

    @abstractclassmethod
    def status(self, task):
        pass

    @abstractclassmethod
    def get_event(self, task):
        pass

    @abstractclassmethod
    def cancel(self, task):
        pass

    @abstractclassmethod
    def parse_url(self, url):
        pass