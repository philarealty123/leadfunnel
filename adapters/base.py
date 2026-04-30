from abc import ABC, abstractmethod


class BaseAdapter(ABC):
    VERSION = "0.0.0"

        def __init__(self, source_config, engine=None):
                self.cfg = source_config
                        self.source_id = source_config["id"]
                                self.engine = engine

                                    @abstractmethod
                                        def discover(self):
                                                ...

                                                    @abstractmethod
                                                        def fetch(self, target):
                                                                ...

                                                                    @abstractmethod
                                                                        def parse(self, raw):
                                                                                ...

                                                                                    @abstractmethod
                                                                                        def normalize(self, raw_record):
                                                                                                ...

                                                                                                    def run(self):
                                                                                                            for target in self.discover():
                                                                                                                        raw = self.fetch(target)
                                                                                                                                    for rec in self.parse(raw):
                                                                                                                                                    yield self.normalize(rec)