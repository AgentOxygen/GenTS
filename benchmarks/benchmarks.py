from gents.hfcollection import HFCollection, sort_hf_groups
from gents.timeseries import TSCollection
from gents.mhfdataset import MHFDataset
from gents.meta import get_attributes
from benchmarks.fixtures import build_bench_case
from os import listdir, makedirs
from pathlib import Path

SIMPLE_SUITE_NUM_HIST_FILES = 100

class SimpleSuite:
    def setup(self):
        self.hf_head_dir = "hf/"
        self.ts_head_dir = "ts/"
        makedirs(self.ts_head_dir, exist_ok=True)

        self.hf_paths = build_bench_case(self.hf_head_dir, n_files=SIMPLE_SUITE_NUM_HIST_FILES, n_steps=1)

    def time_hfcollection_create(self):
        hfc = HFCollection(self.hf_head_dir)

    def time_hfcollection_pull(self):
        hfc = HFCollection(self.hf_head_dir)
        hfc.pull_metadata()

    def time_tscollection_create(self):
        hfc = HFCollection(self.hf_head_dir)
        tsc = TSCollection(hfc, self.ts_head_dir)
    
    def time_tscollection_execute(self):
        hfc = HFCollection(self.hf_head_dir)
        tsc = TSCollection(hfc, self.ts_head_dir)
        tsc.execute()

    def time_mhfdataset(self):
        with MHFDataset(self.hf_paths) as agg_hf_ds:
            global_attrs = agg_hf_ds.get_global_attrs()
            data_vals = agg_hf_ds.get_var_vals("VAR0")
            assert data_vals is not None


LARGE_GROUP_NUM_HIST_FILES = 40
LARGE_GROUP_NUM_TIMESTEPS = 2000


class LargeGroupSuite:
    """Stresses the per-group timestep-delta computation in ``pull_metadata`` with
    a single group holding many multi-step history files (a large collection of
    total time steps)."""

    def setup(self):
        self.hf_head_dir = "hf_large/"
        self.hf_paths = build_bench_case(self.hf_head_dir, n_files=LARGE_GROUP_NUM_HIST_FILES, n_steps=LARGE_GROUP_NUM_TIMESTEPS)

    def time_hfcollection_pull(self):
        hfc = HFCollection(self.hf_head_dir)
        hfc.pull_metadata()


SORT_NUM_DIRS = 8
SORT_STREAMS_PER_DIR = 24
SORT_FILES_PER_STREAM = 250


class GroupSortSuite:
    """Stresses ``sort_hf_groups`` with a wide tree: many directories, each holding
    many output streams. Cost here is pure path-string work, so no files are
    written -- the grouping never touches the filesystem."""

    def setup(self):
        self.hf_paths = sorted(
            Path(f"/case/comp{d:02d}/hist/b.e30.BHIST.ne30.{d:03d}.cam.h{s}."
                 f"{1850 + f // 12:04d}-{f % 12 + 1:02d}.nc")
            for d in range(SORT_NUM_DIRS)
            for s in range(SORT_STREAMS_PER_DIR)
            for f in range(SORT_FILES_PER_STREAM)
        )

    def time_sort_hf_groups(self):
        groups = sort_hf_groups(self.hf_paths)
        assert len(groups) == SORT_NUM_DIRS * SORT_STREAMS_PER_DIR