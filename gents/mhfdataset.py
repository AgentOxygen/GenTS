from gents.datastore import GenTSDataStore
from pathlib import Path
from gents.meta import get_attributes, get_time_variables_names
from cftime import num2date
import numpy as np


def get_concat_coords(hf_datasets):
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
    dim_coords = {}

    for ds in hf_datasets:
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

    def __init__(self, hf_paths):
        """
        Stores the history file paths and initialises empty internal state.

        No files are opened at construction time; call :meth:`open` or use the
        instance as a context manager.

        :param hf_paths: Paths to the history files that form this group.
        :type hf_paths: list[str or pathlib.Path]
        """
        self.__hf_files = [Path(path) for path in hf_paths]
        self.__hf_datasets = None
        self.__time_mapping = {}
        self.__data_coords = None

    def open(self):
        """
        Opens all history file handles and builds the internal time mapping.

        Constructs ``__time_mapping``: a dictionary from each unique float time
        value to the list of file indices that contain it.  Raises an exception if
        the number of files per time step is not consistent across all time values
        (i.e. fragmentation is inconsistent).

        :raises Exception: If the spatial fragmentation is not consistent over time.
        """
        if self.__hf_datasets is None:
            self.__hf_datasets = [GenTSDataStore(path, 'r') for path in self.__hf_files]
            self.__time_name, self.time_bnds_name = get_time_variables_names(self.__hf_datasets[0])
            self.__time_vals = [np.squeeze(hf_data[self.__time_name][:]) for hf_data in self.__hf_datasets]

            for hf_index in range(len(self.__hf_datasets)):
                time_vals = self.__time_vals[hf_index]
                if len(time_vals.shape) == 0:
                    time_vals = [float(time_vals)]
                
                for time in time_vals:
                    time = float(time)
                    if time in self.__time_mapping:
                        self.__time_mapping[time].append(hf_index)
                    else:
                        self.__time_mapping[time] = [hf_index]
            if not self.is_time_consistent():
                raise Exception("Fragmentation is not consistent over time.")

    def close(self):
        """
        Closes all open netCDF4 file handles.
        """
        if self.__hf_datasets is not None:
            for ds in self.__hf_datasets:
                ds.close()

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
        init_ds = self.__hf_datasets[0][var_name]
        return list(init_ds.dimensions)

    def get_var_dtype(self, var_name):
        """
        Returns the NumPy dtype of a variable, read from the first file in the group.

        :param var_name: Name of the variable to inspect.
        :type var_name: str
        :returns: NumPy dtype of the variable.
        :rtype: numpy.dtype
        """
        return self.__hf_datasets[0][var_name].dtype

    def get_var_attrs(self, var_name):
        """
        Returns the attribute dictionary for a variable from the first file in the group.

        :param var_name: Name of the variable to inspect.
        :type var_name: str
        :returns: Dictionary mapping attribute names to their values.
        :rtype: dict
        """
        return get_attributes(self.__hf_datasets[0][var_name])

    def __check_coord_map(self):
        """
        Lazily initialises the combined coordinate map for all dimensions.

        For non-fragmented groups, builds the map from the first file's dimensions
        plus the aggregated time values from :meth:`get_time_vals`.  For fragmented
        groups, delegates to :func:`get_concat_coords` to merge spatial coordinates
        across all tiles.  Has no effect if the map has already been built.
        """
        if self.__data_coords is None:
            if self.is_fragmented():
                self.__data_coords = get_concat_coords(self.__hf_datasets)
            else:
                self.__data_coords = {}
                init_ds = self.__hf_datasets[0]
                for dim in init_ds.dimensions:
                    if dim in init_ds.variables:
                        self.__data_coords[dim] = init_ds[dim][:]
                    else:
                        self.__data_coords[dim] = np.arange(init_ds.dimensions[dim].size)
                self.__data_coords[self.__time_name] = self.get_time_vals()

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
        self.__check_coord_map()
        if var_name in self.__data_coords:
            return [len(self.__data_coords[var_name])]
        else:
            init_ds = self.__hf_datasets[0][var_name]
            dim_shape = []
            if self.__time_name in init_ds.dimensions:
                dim_shape.append(len(self.get_time_vals()))
            dim_shape += [len(self.__data_coords[dim]) for dim in init_ds.dimensions if dim != self.__time_name]
            return dim_shape

    def get_var_vals(self, var_name, time_index_start=0, time_index_end=None):
        """
        Reads and returns a variable's data across the group for a time slice.

        Two execution paths are used depending on fragmentation:

        - **Non-fragmented:** iterates over the requested time values and reads
          each time step from the appropriate single file.
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
        self.__check_coord_map()
        if var_name in self.__data_coords:
            return self.__data_coords[var_name]

        if "time" not in self.get_var_dimensions(var_name):
            return self.__hf_datasets[0][var_name][:]

        time_vals = self.get_time_vals()[time_index_start:time_index_end]
        data_shape = self.get_var_data_shape(var_name)
        data_shape[0] = len(time_vals)

        var_vals = np.empty(data_shape, dtype=self.__hf_datasets[0][var_name].dtype)
        if not self.is_fragmented():
            for index, time_val in enumerate(time_vals):
                hf_index = self.__time_mapping[time_val][0]
                hf_data = self.__hf_datasets[hf_index]
                if hf_data[self.__time_name].shape[0] > 1:
                    sub_t_index = np.where(self.__time_vals[hf_index] == time_val)[0]
                    var_vals[index] = hf_data[var_name][sub_t_index]
                else:
                    var_vals[index] = hf_data[var_name][:]
        else:
            for time_index, time_val in enumerate(time_vals):
                for hf_index in self.__time_mapping[time_val]:
                    hf_data = self.__hf_datasets[hf_index]
                    if self.__time_name in hf_data[var_name].dimensions and hf_data[self.__time_name].shape[0] > 1:
                        sub_t_index = np.where(self.__time_vals[hf_index] == time_val)[0]
                        hf_data_fragment = hf_data[var_name][sub_t_index]
                    else:
                        hf_data_fragment = hf_data[var_name][:]
                    
                    index_ranges = []
                    for dim_index, dim in enumerate(hf_data[var_name].dimensions):
                        if dim == self.__time_name:
                            index_ranges.append(time_index)
                        elif dim in hf_data.variables:
                            dim_vals = hf_data[dim][:]
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
        assert self.__hf_datasets is not None

        agg_attrs = {}
        for ds in self.__hf_datasets:
            agg_attrs |= get_attributes(ds)
        return agg_attrs

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def __contains__(self, key):
        return key in self.__hf_files

    def __iter__(self):
        return iter(self.__hf_datasets)

    def __len__(self):
        return len(self.__hf_datasets)
    
    def __getitem__(self, item):
        return self.__hf_datasets[item]