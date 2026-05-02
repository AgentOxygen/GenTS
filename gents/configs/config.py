from gents.hfcollection import HFCollection
from gents.timeseries import TSCollection


class GenTSConfig:
    hf_include_patterns = ["*.nc"]
    hf_exclude_patterns = ["*.log"]

    def __init__(self, input_dir, output_dir):
        self._input_dir = input_dir
        self._output_dir = output_dir

    def get_hfcollection(self, num_cores, slice_size_years=10, slice_start_year=None, align_method="midpoint"):
        hfc = HFCollection(self._input_dir , num_processes=num_cores)
        hfc = hfc.include(self.hf_include_patterns).exclude(self.hf_exclude_patterns)
        hfc = hfc.slice_groups(
            slice_size_years=slice_size_years,
            start_year=slice_start_year,
            time_alignment_method=align_method
        )
        return hfc

    def get_tscollection(self, hfc, num_cores, append_dirs=True, overwrite=False):
        tsc = TSCollection(hfc, self._output_dir , num_processes=num_cores)
        if append_dirs:
            tsc = tsc.append_timestep_dirs()
        if overwrite:
            tsc = tsc.apply_overwrite("*")
        return tsc