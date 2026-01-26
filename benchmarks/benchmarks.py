from gents.hfcollection import HFCollection
from gents.timeseries import TSCollection
from gents.utils import generate_history_file
from os import listdir, makedirs

SIMPLE_SUITE_NUM_HIST_FILES = 100

class SimpleSuite:
    def setup(self):
        self.hf_head_dir = "hf/"
        self.ts_head_dir = "ts/"

        makedirs(self.hf_head_dir, exist_ok=True)
        makedirs(self.ts_head_dir, exist_ok=True)

        self.hf_paths = [f"{self.hf_head_dir}/benchmark.hf.{str(index).zfill(5)}.nc" for index in range(SIMPLE_SUITE_NUM_HIST_FILES)]

        for file_index, path in enumerate(self.hf_paths):
            generate_history_file(path, [(file_index+1)*30], [[file_index*30, (file_index+1)*30]])
    
    def time_hfcollection_create(self):
        hfc = HFCollection(self.hf_head_dir)

    def time_hfcollection_pull(self):
        hfc = HFCollection(self.hf_head_dir)
        hfc.pull_metadata()

    def time_tscollection_create(self):
        hfc = HFCollection(self.hf_head_dir)
        tsc = TSCollection(hfc, self.ts_head_dir)
