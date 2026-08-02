from gents.datastore import GenTSDataStore
from pathlib import Path
from gents.meta import netCDFMeta
import numpy as np


def extend_coords(ds, dim_coords={}):
    """
    Builds a combined coordinate map across all datasets in a spatially fragmented group.

    For each dimension across all open datasets:

    - If the dimension has a coordinate variable, its values are merged and
      de-duplicated with ``numpy.unique`` across all files.
    - If there is no coordinate variable, a 0-indexed integer range matching
      the dimension size is used.

    :param hf_datasets: List of open ``netCDF4.Dataset`` objects from the group.
    :type hf_datasets: list[netCDF4.Dataset]
    :returns: Dictionary mapping dimension names to their combined coordinate arrays.
    :rtype: dict
    """
    for dim in ds.dimensions:
        if dim in ds.variables and dim in dim_coords:
            dim_coords[dim] = np.unique(np.concat([ds[dim][:], dim_coords[dim]]))
        elif dim in ds.variables:
            dim_coords[dim] = ds[dim][:]
        else:
            dim_coords[dim] = np.arange(ds.dimensions[dim].size)

    return dim_coords


class MHFDataset:
    """
    Aggregating dataset interface over a group of related history files.

    Presents multiple history files — covering the same time range and/or
    different spatial tiles — as a single virtual dataset.  All file handles
    are opened together on :meth:`open` (or ``__enter__``) and closed together
    on :meth:`close` (or ``__exit__``).
    """

    def __init__(self, hf_paths, preload_var_list=None, memory_limit_bytes=np.inf, load_secondaries=True):
        """
        Stores the history file paths and initialises empty internal state.

        No files are opened at construction time; call :meth:`open` or use the
        instance as a context manager.

        :param hf_paths: Paths to the history files that form this group.
        :type hf_paths: list[str or pathlib.Path]
        """
        self.__hf_files = [Path(path) for path in hf_paths]
        self.__hf_metas = []
        self.__time_mapping = {}
        self.__time_name = None
        self.time_bnds_name = None
        self.__data_coords = {}
        self.__data_secondary_var_cache = {}
        self.__data_var_cache = {}
        self.__last_var_read = None
        self.__memory_limit_bytes = memory_limit_bytes
        self.__past_vars_read = []
        self.__load_secondaries = load_secondaries
        self.__preload_var_list = list(preload_var_list) if preload_var_list is not None else []

    def open(self):
        """
        Opens all history file handles and builds the internal time mapping.

        Constructs ``__time_mapping``: a dictionary from each unique float time
        value to the list of ``(file_index, sub_time_index)`` pairs that
        contain it -- ``sub_time_index`` is the position of that time value
        within *its own file's* time array (``0`` for single-step files),
        precomputed here so :meth:`get_var_vals` never has to re-scan a
        file's time array to find it. Raises an exception if the number of
        files per time step is not consistent across all time values (i.e.
        fragmentation is inconsistent).

        Secondary-variable data (``time``, ``time_bnds``, and -- for a
        fragmented group -- per-tile ``lat``/``lon``) can differ from file to
        file, so it is accumulated per file here rather than read once from
        the first file and reused; these arrays are small compared to primary
        variable data, so the extra reads are cheap. Primary variables named
        in ``preload_var_list`` are read in this same per-file pass too, so
        :meth:`get_var_vals` can serve them straight from cache instead of
        reopening every file a second time.

        :raises Exception: If the spatial fragmentation is not consistent over time.
        """
        for hf_index, path in enumerate(self.__hf_files):
            with GenTSDataStore(path, 'r') as hf_ds:
                # MHFDataset only ever consumes raw float times (self.__time_mapping)
                # -- never the decoded CFTime values -- so skip the num2date cost.
                # time_bnds data and dimension bounds are read/computed by this
                # class itself (secondary-var cache, extend_coords) -- loading
                # them again inside netCDFMeta would be a pure duplicate read.
                # Variable attributes are only ever consulted from the first
                # file in the group (get_var_attrs() below), so only load them
                # there.
                hf_meta = netCDFMeta(
                    hf_ds, path,
                    decode_dates=False, load_time_bounds=False, compute_dim_bounds=False,
                    load_variable_attrs=(hf_index == 0),
                )

                self.__data_coords = extend_coords(hf_ds, self.__data_coords)

                if self.__time_name is None or self.time_bnds_name is None:
                    self.__time_name = hf_meta.get_time_var_name()
                    self.time_bnds_name = hf_meta.get_timebnds_var_name()

                hf_ds.set_auto_maskandscale(False)

                for sub_t_index, time in enumerate(hf_meta.get_float_times()):
                    time = float(time)
                    if time in self.__time_mapping:
                        self.__time_mapping[time].append((hf_index, sub_t_index))
                    else:
                        self.__time_mapping[time] = [(hf_index, sub_t_index)]

                if self.__load_secondaries:
                    for var_name in hf_meta.get_secondary_variables():
                        if var_name in self.__data_secondary_var_cache:
                            self.__data_secondary_var_cache[var_name].append(hf_ds[var_name][:])
                        else:
                            self.__data_secondary_var_cache[var_name] = [hf_ds[var_name][:]]

                for var_name in self.__preload_var_list:
                    # Callers may pass sentinel/non-variable names (e.g. TSCollection's
                    # "auxiliary" pseudo-order for groups with no primary variables) --
                    # only preload names that are actually primary variables in this file.
                    if var_name not in hf_meta.get_primary_variables():
                        continue
                    if var_name in self.__data_var_cache:
                        self.__data_var_cache[var_name].append(hf_ds[var_name][:])
                    else:
                        self.__data_var_cache[var_name] = [hf_ds[var_name][:]]

                self.__hf_metas.append(hf_meta)

        if not self.is_time_consistent():
            raise Exception("Fragmentation is not consistent over time.")

    def close(self):
        """
        Drops all cached variable data, leaving the instance ready to be reopened.
        """
        self.__data_var_cache = {}
        self.__data_secondary_var_cache = {}

    def __cache_variable(self, target_var_name, cache_ahead=True):
        vars_to_cache = [target_var_name]
        # Check memory size of cache + the target variable to cache
        cache_size_b = self.get_var_dsize(target_var_name)
        for var_name in self.__data_var_cache:
            cache_size_b += self.get_var_dsize(var_name)

        if cache_size_b > self.__memory_limit_bytes:
            raise MemoryError(f"Cache size of {cache_size_b / (1024**3)}GB with '{target_var_name}' exceeds limit of {self.__memory_limit_bytes / (1024**3)}GB.")
        
        if cache_ahead:
            for nvar_name in self.__hf_metas[0].get_primary_variables():
                if self.get_var_dsize(nvar_name) + cache_size_b > self.__memory_limit_bytes:
                    continue
                if nvar_name not in self.__past_vars_read and nvar_name not in vars_to_cache:
                    cache_size_b += self.get_var_dsize(nvar_name)
                    vars_to_cache.append(nvar_name)

        # if the variable has no time dimension, no reason to iterate over all of the history files
        # we tackle those first (if they exist)
        notime_vars_to_cache = []
        time_vars_to_cache = []
        for var_name in vars_to_cache:
            if self.__time_name in self.get_var_dimensions(var_name):
                time_vars_to_cache.append(var_name)
            else:
                notime_vars_to_cache.append(var_name)

        # Iterate over history files
        for index, path in enumerate(self.__hf_files):
            with GenTSDataStore(path, 'r') as hf_ds:
                if index == 0:
                    for var_name in notime_vars_to_cache:
                        self.__data_var_cache[var_name] = hf_ds[var_name][:]
                
                for var_name in time_vars_to_cache:
                    if var_name in self.__data_var_cache:
                        self.__data_var_cache[var_name].append(hf_ds[var_name][:])
                    else:
                        self.__data_var_cache[var_name] = [hf_ds[var_name][:]]

    def __get_hf_data(self, index, var_name):
        hf_path = self.__hf_files[index]

        if var_name in self.__data_secondary_var_cache:
            return self.__data_secondary_var_cache[var_name][index]

        # Check if done reading last variable and delete if so
        if self.__last_var_read != var_name:
            if self.__last_var_read is not None:
                del self.__data_var_cache[self.__last_var_read]
            self.__past_vars_read.append(self.__last_var_read)
            self.__last_var_read = var_name
        
        # Check cache, if its in the cache, return it
        if var_name in self.__data_var_cache:
            return self.__data_var_cache[var_name][index]
        # If its not in the cache, this is likely a new block of variables
        # so, check if the entire timeseries for variable fits in cache and if possible put it there
        # also put the other variables that will fit.
        else:
            var_memory_size = self.get_var_dsize(var_name)

            if var_memory_size < self.__memory_limit_bytes:
                # cache this variable + others to fill it, then return from cache
                self.__cache_variable(var_name)
                return self.__data_var_cache[var_name][index]
            else:
                # doesnt fit in memory, so don't cache
                # we could partially cache the timeseries, but for now, I will just skip caching the variable all together
                with GenTSDataStore(hf_path, 'r') as hf_ds:
                    return hf_ds[var_name][:]

    def get_var_dsize(self, var_name):
        return np.prod(self.get_var_data_shape(var_name))*self.get_var_dtype(var_name).itemsize

    def get_time_vals(self):
        """
        Returns the sorted array of unique float time values across the group.

        :returns: 1-D array of sorted, unique float time values.
        :rtype: numpy.ndarray
        """
        vals = list(self.__time_mapping.keys())
        return np.sort(np.array(vals))

    def is_time_consistent(self):
        """
        Checks that every time step is covered by the same number of files.

        Required for spatially fragmented groups to ensure every tile is present
        for every time step.

        :returns: ``True`` if all time values have the same fragment count,
            ``False`` otherwise.
        :rtype: bool
        """
        n_time_files = len(self.__time_mapping[self.get_time_vals()[0]])
        for time in self.__time_mapping:
            if len(self.__time_mapping[time]) != n_time_files:
                return False
        return True

    def is_fragmented(self):
        """
        Returns whether the group consists of spatially fragmented (tiled) files.

        :returns: ``True`` if the first time value is covered by more than one file,
            ``False`` otherwise.
        :rtype: bool
        """
        init_time = self.get_time_vals()[0]
        if len(self.__time_mapping[init_time]) > 1:
            return True
        return False

    def get_var_dimensions(self, var_name):
        """
        Returns the dimension names for a variable, read from the first file in the group.

        :param var_name: Name of the variable to inspect.
        :type var_name: str
        :returns: List of dimension name strings in the order they appear on the variable.
        :rtype: list[str]
        """
        return self.__hf_metas[0].get_variable_dims(var_name)

    def get_var_dtype(self, var_name):
        """
        Returns the NumPy dtype of a variable, read from the first file in the group.

        :param var_name: Name of the variable to inspect.
        :type var_name: str
        :returns: NumPy dtype of the variable.
        :rtype: numpy.dtype
        """
        return self.__hf_metas[0].get_variable_dtype(var_name)

    def get_var_attrs(self, var_name):
        """
        Returns the attribute dictionary for a variable from the first file in the group.

        :param var_name: Name of the variable to inspect.
        :type var_name: str
        :returns: Dictionary mapping attribute names to their values.
        :rtype: dict
        """
        return self.__hf_metas[0].get_variable_attrs(var_name)

    def get_var_data_shape(self, var_name):
        """
        Returns the full expected output shape of a variable across the entire group.

        Accounts for the total number of aggregated time steps and, for fragmented
        groups, the combined spatial extents.  Returns a single-element list for
        coordinate variables.

        :param var_name: Name of the variable to inspect.
        :type var_name: str
        :returns: List of dimension sizes representing the aggregated output shape.
        :rtype: list[int]
        """
        init_meta = self.__hf_metas[0]
        if var_name in self.__data_coords:
            return [len(self.__data_coords[var_name])]
        else:
            dim_shape = []
            if self.__time_name in init_meta.get_variable_dims(var_name):
                dim_shape.append(len(self.get_time_vals()))
            dim_shape += [len(self.__data_coords[dim]) for dim in init_meta.get_variable_dims(var_name) if dim != self.__time_name]
            return dim_shape

    def get_var_vals(self, var_name, time_index_start=0, time_index_end=None):
        """
        Reads and returns a variable's data across the group for a time slice.

        Two execution paths are used depending on fragmentation:

        - **Non-fragmented:** reads maximal runs of consecutive requested time
          steps that land in the same file at consecutive positions in one
          slice read each, rather than one read per time step.
        - **Fragmented:** for each time step, reads from all spatial-tile files
          and inserts each tile into the correct slice of a pre-allocated output
          array by matching tile coordinate values against the combined coordinate
          map.

        :param var_name: Name of the variable to read.
        :type var_name: str
        :param time_index_start: Index of the first time step to include (inclusive).
            Defaults to ``0``.
        :type time_index_start: int
        :param time_index_end: Index of the last time step to include (exclusive).
            Defaults to ``None`` (all remaining time steps).
        :type time_index_end: int or None
        :returns: Array containing the variable data for the requested time slice.
        :rtype: numpy.ndarray
        """
        if var_name in self.__data_coords:
            return self.__data_coords[var_name]

        if "time" not in self.get_var_dimensions(var_name):
            return self.__get_hf_data(0, var_name)

        time_vals = self.get_time_vals()[time_index_start:time_index_end]
        data_shape = self.get_var_data_shape(var_name)
        data_shape[0] = len(time_vals)

        var_vals = np.empty(data_shape, dtype=self.__hf_metas[0].get_variable_dtype(var_name))
        if not self.is_fragmented():
            n = len(time_vals)
            index = 0
            while index < n:
                hf_index, sub_t_index = self.__time_mapping[time_vals[index]][0]
                run_len = 1
                while index + run_len < n:
                    next_hf_index, next_sub_t_index = self.__time_mapping[time_vals[index + run_len]][0]
                    if next_hf_index != hf_index or next_sub_t_index != sub_t_index + run_len:
                        break
                    run_len += 1
                var_data = self.__get_hf_data(hf_index, var_name)
                var_vals[index:index + run_len] = var_data[sub_t_index:sub_t_index + run_len]
                index += run_len
        else:
            for time_index, time_val in enumerate(time_vals):
                for hf_index, sub_t_index in self.__time_mapping[time_val]:
                    var_data = self.__get_hf_data(hf_index, var_name)
                    if self.__time_name in self.get_var_dimensions(var_name) and self.get_var_data_shape(self.__time_name)[0] > 1:
                        hf_data_fragment = var_data[sub_t_index]
                    else:
                        hf_data_fragment = var_data[0]
                    
                    index_ranges = []
                    for dim_index, dim in enumerate(self.get_var_dimensions(var_name)):
                        if dim == self.__time_name:
                            index_ranges.append(time_index)
                        elif dim in self.__hf_metas[0].get_variables():
                            dim_vals = self.__get_hf_data(hf_index, dim)
                            lower_index = np.where(np.min(dim_vals) == self.__data_coords[dim])[0][0]
                            upper_index = np.where(np.max(dim_vals) == self.__data_coords[dim])[0][0]
                            if lower_index == upper_index:
                                index_ranges.append(lower_index)
                            else:
                                index_ranges.append(slice(lower_index, upper_index+1))
                        elif var_vals.shape[dim_index] == 1:
                            index_ranges.append(0)
                        else:
                            index_ranges.append(slice(0, var_vals.shape[dim_index]))
                    
                    var_vals[tuple(index_ranges)] = np.squeeze(hf_data_fragment)
        return var_vals

    def get_global_attrs(self):
        """
        Returns a merged dictionary of global attributes from all files in the group.

        Attributes from later files overwrite those from earlier files when keys
        conflict.

        :returns: Dictionary mapping global attribute names to their values.
        :rtype: dict
        """
        assert self.__hf_metas is not None

        agg_attrs = {}
        for hf_meta in self.__hf_metas:
            agg_attrs |= hf_meta.get_attributes()
        return agg_attrs

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def __contains__(self, key):
        return key in self.__hf_files

    def __len__(self):
        return len(self.__hf_files)
    
    def __getitem__(self, item):
        return self.__hf_files[item]