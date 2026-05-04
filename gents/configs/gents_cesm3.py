from gents.configs.config import GenTSConfig
from gents.hfcollection import HFCollection
from gents.timeseries import TSCollection


class CESM3Config(GenTSConfig):
    hf_include_patterns = [
        "*/atm/*",
        "*/ice/*",
        "*/lnd/*",
        "*/glc/*",
        "*/ocn/*",
        "*/rof/*",
        "*.nc"
    ]
    hf_exclude_patterns = [
        "*/proc/tseries/*",
        "*/rest/*",
        "*/logs/*",
        "*.ocean_geometry.nc",
        "*mom6.ic.*",
        "*cam.i.*",
        "*.static.*"
    ]

    def get_hfcollection(self, num_cores, slice_size_years=10, slice_start_year=None, align_method="midpoint"):
        hfc = HFCollection(self._input_dir, num_processes=num_cores)
        hfc = hfc.include(self.hf_include_patterns).exclude(self.hf_exclude_patterns)
        hfc = hfc.slice_groups(
            slice_size_years=slice_size_years,
            start_year=slice_start_year,
            time_alignment_method=align_method
        )
        return hfc

    def get_tscollection(self, hfc, num_cores, append_dirs=True, overwrite=False):
        tsc = super().get_tscollection(hfc, num_cores, append_dirs=append_dirs, overwrite=overwrite)
        tsc = tsc.apply_path_swap("/hist/", "/proc/tseries/")
        tsc = tsc.apply_compression(2, "zlib", "*", "*")
        return tsc