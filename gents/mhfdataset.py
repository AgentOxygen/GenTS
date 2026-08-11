#!/usr/bin/env python
"""
Virtual dataset presenting a group of history files as one aggregated whole.

Developer: Cameron Cummins
Contact: cameron.cummins@utexas.edu
"""
from gents.datastore import GenTSDataStore
from pathlib import Path
from gents.meta import netCDFMeta
import numpy as np


def extend_coords(ds, dim_coords={}):
    """
    Folds one dataset's coordinates into a combined coordinate map.

    Called once per file to build the group's full extent: coordinate values are
    merged and de-duplicated across files, and a dimension with no coordinate
    variable gets a 0-indexed integer range instead.

    :param ds: Open dataset to merge in.
    :type ds: gents.datastore.GenTSDataStore or netCDF4.Dataset
    :param dim_coords: Coordinate map accumulated from earlier files.
    :type dim_coords: dict
    :returns: The updated ``{dimension: coordinate array}`` map.
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

    Presents files covering successive time steps and/or different spatial tiles
    as one virtual dataset. Files are opened one at a time -- on :meth:`open` (or
    ``__enter__``) to read metadata and fill the cache, and again on demand for
    data that did not fit -- so the number of open handles never scales with the
    size of the group.

    Data is served from an in-memory cache keyed by variable. :meth:`open` fills
    it with the secondary variables and as many of ``preload_var_list`` as fit
    under ``memory_limit_bytes``; anything else is read (and cached, if it fits)
    on first use. A variable's cache is dropped when reads move on to the next
    variable.
    """

    def __init__(self, hf_paths, preload_var_list=None, memory_limit_bytes=np.inf, load_secondaries=True, preload_primaries=True):
        """
        Stores the group's paths; opens nothing until :meth:`open` is called.

        :param hf_paths: Paths to the history files that form this group.
        :type hf_paths: list[str or pathlib.Path]
        :param preload_var_list: Primary variables to cache during :meth:`open`,
            in priority order. Remaining primaries are appended after them.
        :type preload_var_list: list[str] or None
        :param memory_limit_bytes: Ceiling on cached variable data. Unbounded by
            default.
        :type memory_limit_bytes: float
        :param load_secondaries: Cache secondary variable data during :meth:`open`.
        :type load_secondaries: bool
        :param preload_primaries: Cache primary variable data during :meth:`open`.
            ``False`` skips all primary reads up front (``preload_var_list`` is
            ignored); primaries are still read on demand. Used by ``no_data``
            runs, which never read primary data at all.
        :type preload_primaries: bool
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
        self.__preload_primaries = preload_primaries
        self.__preload_var_list = list(preload_var_list) if preload_var_list is not None else []
        self.__sorted_time_vals = None
        self.__fragmented = None

    def __estimate_var_bytes(self, var_name, hf_meta):
        """
        Estimates a variable's aggregated size across the group by scaling one
        file's shape by the file count.

        Exact when every file holds the same number of time steps; approximate
        otherwise. Used only to plan the preload cache before the group has been
        scanned.
        """
        shape = hf_meta.get_variable_shapes(var_name)
        itemsize = hf_meta.get_variable_dtype(var_name).itemsize
        return int(np.prod(shape)) * itemsize * len(self.__hf_files)

    def __plan_cacheable_vars(self, candidate_names, hf_meta, running_total=0):
        """
        Decides which of ``candidate_names`` (in priority order) fit under
        ``memory_limit_bytes``, given the bytes already committed elsewhere.

        All-or-nothing per variable: a variable is cached for the whole group or
        not at all. Downstream code assumes a cache entry holds one array per
        file, so a partial entry would serve the wrong file's data.
        """
        fit = []
        for var_name in candidate_names:
            estimated_bytes = self.__estimate_var_bytes(var_name, hf_meta)
            if running_total + estimated_bytes > self.__memory_limit_bytes:
                continue
            running_total += estimated_bytes
            fit.append(var_name)
        return fit

    def open(self):
        """
        Reads every file in the group once, building the time mapping and cache.

        ``__time_mapping`` maps each unique float time value to the
        ``(file_index, sub_time_index)`` pairs holding it, where
        ``sub_time_index`` is that value's position within its own file's time
        array -- precomputed so :meth:`get_var_vals` never rescans a time array.

        Secondary variables are cached per file rather than read once from the
        first file, since time, bounds and per-tile coordinates differ between
        files; they are small enough for that to be cheap. Preloaded primaries
        ride along in the same pass so no file has to be reopened for them.

        :raises Exception: If the spatial fragmentation is not consistent over time.
        """
        vars_to_cache_secondary = None
        vars_to_cache_primary = None

        for hf_index, path in enumerate(self.__hf_files):
            with GenTSDataStore(path, 'r') as hf_ds:
                hf_meta = netCDFMeta(
                    hf_ds, path,
                    decode_dates=False, load_time_bounds=False, compute_dim_bounds=False,
                    load_variable_attrs=(hf_index == 0),
                )
                if hf_index == 0:
                    # Drop secondaries (cached separately) and reject names the
                    # group does not actually have.
                    checked_preload_list = []
                    for var_name in self.__preload_var_list:
                        if self.__load_secondaries and var_name in hf_meta.get_secondary_variables():
                            continue
                        if var_name not in hf_meta.get_variables():
                            raise KeyError(f"Attempted to cache non-existent variable '{var_name}'.")
                        checked_preload_list.append(var_name)
                    self.__preload_var_list = checked_preload_list

                    # Anything the caller did not name is still worth caching if
                    # it fits, so queue the rest of the group's primaries behind it.
                    if self.__preload_primaries:
                        for var_name in hf_meta.get_primary_variables():
                            if var_name not in self.__preload_var_list:
                                self.__preload_var_list.append(var_name)
                    else:
                        self.__preload_var_list = []

                    secondary_names = list(hf_meta.get_secondary_variables()) if self.__load_secondaries else []
                    vars_to_cache_secondary = self.__plan_cacheable_vars(secondary_names, hf_meta)
                    secondary_bytes = sum(self.__estimate_var_bytes(name, hf_meta) for name in vars_to_cache_secondary)
                    vars_to_cache_primary = self.__plan_cacheable_vars(self.__preload_var_list, hf_meta, secondary_bytes)

                self.__data_coords = extend_coords(hf_ds, self.__data_coords)

                if self.__time_name is None or self.time_bnds_name is None:
                    self.__time_name = hf_meta.get_time_var_name()
                    self.time_bnds_name = hf_meta.get_timebnds_var_name()

                hf_ds.set_auto_maskandscale(False)

                # tolist() yields plain Python floats in one pass, instead of a
                # per-element masked-array __getitem__ plus float() call.
                float_times = np.ma.getdata(np.atleast_1d(hf_meta.get_float_times())).tolist()
                for sub_t_index, time in enumerate(float_times):
                    if time in self.__time_mapping:
                        self.__time_mapping[time].append((hf_index, sub_t_index))
                    else:
                        self.__time_mapping[time] = [(hf_index, sub_t_index)]

                if self.__load_secondaries:
                    for var_name in vars_to_cache_secondary:
                        if var_name not in hf_meta.get_secondary_variables():
                            continue
                        if var_name in self.__data_secondary_var_cache:
                            self.__data_secondary_var_cache[var_name].append(hf_ds[var_name][:])
                        else:
                            self.__data_secondary_var_cache[var_name] = [hf_ds[var_name][:]]

                for var_name in vars_to_cache_primary:
                    if var_name not in hf_meta.get_primary_variables():
                        continue
                    if var_name in self.__data_var_cache:
                        self.__data_var_cache[var_name].append(hf_ds[var_name][:])
                    else:
                        self.__data_var_cache[var_name] = [hf_ds[var_name][:]]

                self.__hf_metas.append(hf_meta)

        # The mapping is final now, so the sorted time axis and fragmentation
        # flag are computed once here rather than re-derived on every call.
        self.__sorted_time_vals = np.sort(np.array(list(self.__time_mapping.keys())))
        self.__fragmented = len(self.__time_mapping[self.__sorted_time_vals[0]]) > 1

        if not self.is_time_consistent():
            raise Exception("Fragmentation is not consistent over time.")

    def close(self):
        """
        Drops all cached variable data, leaving the instance ready to be reopened.
        """
        self.__data_var_cache = {}
        self.__data_secondary_var_cache = {}

    def __get_cache_dsize(self):
        """
        Returns the total number of bytes currently held in the caches.

        :rtype: int
        """
        cache_size_b = 0
        for var_name in self.__data_var_cache:
            for data in self.__data_var_cache[var_name]:
                if data is not None:
                    cache_size_b += data.size * data.dtype.itemsize
        for var_name in self.__data_secondary_var_cache:
            for data in self.__data_secondary_var_cache[var_name]:
                if data is not None:
                    cache_size_b += data.size * data.dtype.itemsize
        return cache_size_b

    def __cache_variable(self, target_var_name, cache_ahead=True):
        """
        Reads ``target_var_name`` across the group into the cache, filling any
        remaining headroom with other not-yet-read variables.

        :raises MemoryError: If the target variable does not fit under the limit.
        """
        vars_to_cache = [target_var_name]
        cache_size_b = self.__get_cache_dsize() + self.get_var_dsize(target_var_name)

        if cache_size_b > self.__memory_limit_bytes:
            raise MemoryError(f"Cache size of {cache_size_b / (1024**3)}GB with '{target_var_name}' exceeds limit of {self.__memory_limit_bytes / (1024**3)}GB.")
        
        if cache_ahead:
            for nvar_name in self.__preload_var_list:
                if self.get_var_dsize(nvar_name) + cache_size_b > self.__memory_limit_bytes:
                    continue
                if nvar_name not in self.__past_vars_read and nvar_name not in vars_to_cache:
                    cache_size_b += self.get_var_dsize(nvar_name)
                    vars_to_cache.append(nvar_name)

        # A variable without a time dimension is identical in every file, so it
        # is read once from the first rather than once per file.
        notime_vars_to_cache = []
        time_vars_to_cache = []
        for var_name in vars_to_cache:
            if self.__time_name in self.get_var_dimensions(var_name):
                time_vars_to_cache.append(var_name)
            else:
                notime_vars_to_cache.append(var_name)

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

    def __get_hf_data(self, index, var_name, time_slice=None):
        """
        Returns one file's data for a variable, from cache where possible.

        :param time_slice: Slice along the file's own time axis to return;
            ``None`` returns the file's full array. Cached data returns a view;
            uncacheable data reads only the sliced region from disk.
        :type time_slice: slice or None
        :rtype: numpy.ndarray
        """
        if var_name in self.__data_secondary_var_cache:
            data = self.__data_secondary_var_cache[var_name][index]
            return data if time_slice is None else data[time_slice]

        # Reads move through one variable at a time, so a switch means the
        # previous variable is finished with and its cache can be released.
        if self.__last_var_read != var_name:
            if self.__last_var_read is not None and self.__last_var_read in self.__data_var_cache:
                self.__data_var_cache[self.__last_var_read] = []
            self.__past_vars_read.append(self.__last_var_read)
            self.__last_var_read = var_name

        # An entry is not freed after a read: write chunks do not align with
        # source file boundaries, so the same file may be read more than once
        # for one variable. Eviction on variable switch (above) bounds growth.
        if var_name in self.__data_var_cache and index < len(self.__data_var_cache[var_name]):
            data = self.__data_var_cache[var_name][index]
            return data if time_slice is None else data[time_slice]
        elif self.__get_cache_dsize() + self.get_var_dsize(var_name) < self.__memory_limit_bytes:
            self.__cache_variable(var_name)
            data = self.__data_var_cache[var_name][index]
            return data if time_slice is None else data[time_slice]
        else:
            # Too big to cache whole; reread just the requested region.
            with GenTSDataStore(self.__hf_files[index], 'r') as hf_ds:
                if time_slice is None:
                    return hf_ds[var_name][:]
                return hf_ds[var_name][time_slice]

    def get_var_dsize(self, var_name):
        """
        Returns the size in bytes of a variable aggregated across the group.

        :param var_name: Name of the variable to size.
        :type var_name: str
        :rtype: int
        """
        return np.prod(self.get_var_data_shape(var_name))*self.get_var_dtype(var_name).itemsize

    def get_time_vals(self):
        """
        Returns the sorted, unique float time values across the group.

        Served from the copy computed at :meth:`open`; callers must not mutate
        the returned array.

        :rtype: numpy.ndarray
        """
        if self.__sorted_time_vals is not None:
            return self.__sorted_time_vals
        vals = list(self.__time_mapping.keys())
        return np.sort(np.array(vals))

    def is_time_consistent(self):
        """
        Returns whether every time step is covered by the same number of files,
        i.e. no spatial tile is missing from any step.

        :rtype: bool
        """
        n_time_files = len(self.__time_mapping[self.get_time_vals()[0]])
        for time in self.__time_mapping:
            if len(self.__time_mapping[time]) != n_time_files:
                return False
        return True

    def is_fragmented(self):
        """
        Returns whether the group is spatially fragmented, i.e. whether its first
        time value is covered by more than one file.

        :rtype: bool
        """
        if self.__fragmented is not None:
            return self.__fragmented
        init_time = self.get_time_vals()[0]
        if len(self.__time_mapping[init_time]) > 1:
            return True
        return False

    def get_var_dimensions(self, var_name):
        """
        Returns a variable's dimension names, taken from the first file in the group.

        :param var_name: Name of the variable to inspect.
        :type var_name: str
        :rtype: tuple[str]
        """
        return self.__hf_metas[0].get_variable_dims(var_name)

    def get_var_dtype(self, var_name):
        """
        Returns a variable's NumPy dtype, taken from the first file in the group.

        :param var_name: Name of the variable to inspect.
        :type var_name: str
        :rtype: numpy.dtype
        """
        return self.__hf_metas[0].get_variable_dtype(var_name)

    def get_var_attrs(self, var_name):
        """
        Returns a variable's attributes, taken from the first file in the group.

        :param var_name: Name of the variable to inspect.
        :type var_name: str
        :rtype: dict
        """
        return self.__hf_metas[0].get_variable_attrs(var_name)

    def get_var_data_shape(self, var_name):
        """
        Returns a variable's aggregated shape across the whole group.

        Accounts for the total number of time steps and, for fragmented groups,
        the combined spatial extent. Coordinate variables get a single-element
        shape.

        :param var_name: Name of the variable to inspect.
        :type var_name: str
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
        Reads a variable's data across the group for a slice of the time axis.

        Non-fragmented groups are read in maximal runs of consecutive time steps
        that fall in the same file, one slice read per run. Fragmented groups are
        assembled step by step, each tile placed into a pre-allocated array by
        matching its coordinates against the group's combined coordinate map.

        :param var_name: Name of the variable to read.
        :type var_name: str
        :param time_index_start: First time step to include (inclusive).
        :type time_index_start: int
        :param time_index_end: Last time step to include (exclusive); ``None``
            reads to the end.
        :type time_index_end: int or None
        :returns: Array of the variable's data over the requested slice.
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
                var_vals[index:index + run_len] = self.__get_hf_data(
                    hf_index, var_name, time_slice=slice(sub_t_index, sub_t_index + run_len)
                )
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
        Returns the global attributes of every file in the group, merged with
        later files winning on conflicting keys.

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