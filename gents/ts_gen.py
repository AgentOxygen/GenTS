#!/usr/bin/env python
"""
genTS.py

Automatic system for generating time-series datasets from history files
efficiently by leveraging the netCDF4 engine and Dask.

Developer: Cameron Cummins
Contact: cameron.cummins@utexas.edu
Last Header Update: 8/22/24
"""
from time import time
import numpy as np
from pathlib import Path
import netCDF4
import cftime
from os.path import isfile, getsize
from os import remove
import dask.distributed
import dask.bag as db
from importlib.metadata import version as getVersion, PackageNotFoundError


def generateReflectiveOutputDirectory(input_head: Path,
                                      output_head: Path,
                                      parent_dir: Path,
                                      swaps: dict = {}) -> str:
    r"""Generates similar directory structure under new head directory.

    This function allows for generating an output directory structure
    that reflects the strucutre of the input directory structure,
    typically containing the history files, specified by `input_head`
    and `parent_dir`. The directory path return can then be created to
    generate a similar structure for the timeseries files.

    The parent directory is compared against the input head directory to
    identify subdirectories. The subdirectories are then appended to the
    output head directory. Swaps are preformed to replace keyword
    subdirectories with a new directory name or sequence of subdirectories.

    Parameters
    ----------
    input_head : Path
        Path for head directory of input directory structure, typically
        the head directory for the history files.
    output_head : Path
        Path for head directory of output directory structure, typically
        the head directory for the timeseries files.
    parent_dir : Path
        Path for to subdirectory under `input_head`, typically containing
        the history files (parent directory).
    swaps: dict
        Dictionary containing keyword swaps for the returned directory
        structure. For example, ``hist`` may replace ``proc/tseries/``
        (e.g. {``hist``: ``proc/tseries/``}) (Default: {}).

    Returns
    -------
    output_dir : str
        New directory structure under `output_head` that reflects `parent_dir`
        and contains any keywords swaps specified by `swaps`.
    """
    raw_sub_dir = str(parent_dir).split(str(input_head))[-1]
    raw_sub_dir_parsed = raw_sub_dir.split("/")
    for key in swaps:
        for index in range(len(raw_sub_dir_parsed)):
            if raw_sub_dir_parsed[index] == key:
                raw_sub_dir_parsed[index] = swaps[key]
                break
    output_dir = str(output_head) + "/"
    for dir in raw_sub_dir_parsed:
        output_dir += dir + "/"
    return output_dir


def solveForGroupsLeftToRight(file_names: list,
                              delimiter: str = ".") -> list:
    r"""Compares file name substrings to identify groups.

    Each string in `file_names` is converted to a string and parsed using
    the delimiter specified by `delimiter`. Each sequence of resutling
    substrings is compared from left to right, until a difference is
    identified. Paths are then grouped based on these differences.

    The ``nc`` substring is excluded from the comparison
    of substrings since all history files will contain this substring.

    Each groups unique substring is returned, but the list of history file
    names for each group is not returned because some groups may be inclusive
    of other groups, resulting in overlap between history files. This is solved
    for in `generateTimeSeriesGroupPaths`.

    Therefore, this function should be called for each unique output directory
    to group history files that may be different by timestep or some other
    unique substring in the file names.

    Parameters
    ----------
    file_names : list(str)
        List of string-like objects containing the names of history files.
    delimiter : str
        String to parse each file name by to produce substrings for
        comparison (Default: '.').

    Returns
    -------
    groups : list(str)
        List of unique substrings identified in file names.
    """
    varying_parsed_names = [str(name).split(delimiter) for name in file_names]

    lengths = {}
    for parsed in varying_parsed_names:
        if len(parsed) in lengths:
            lengths[len(parsed)].append(parsed)
        else:
            lengths[len(parsed)] = [parsed]

    groups = []
    for length_index in lengths:
        parsed_names = np.array(lengths[length_index])
        unique_strs = []
        for i in range(parsed_names.shape[1]):
            unique_strs.append(np.unique(parsed_names[:, i]))
        common_str = ""
        for index in range(len(unique_strs)):
            if len(unique_strs[index]) == 1:
                if unique_strs[index][0] != "nc":
                    common_str += unique_strs[index][0] + delimiter
            else:
                break
        if len(unique_strs[index]) != len(parsed_names):
            for group in unique_strs[index]:
                groups.append(common_str + group + delimiter)
        else:
            groups.append(common_str)
    return groups


def generateTimeSeriesGroupPaths(paths: list,
                                 input_head_dir: Path,
                                 output_head_dir: Path,
                                 dir_name_swaps: dict = {}) -> dict:
    r"""Groups history files together by common substring patterns.

    This function returns a dictionary that stores output templates
    as keys and their corresponding history file paths as values.
    Each output template is an incomplete path with a full parent
    directory and partial file name. The parent directory path indicates
    which directory the timeseries files produced by the respective history
    file paths (stored in a list as the value to the output template which
    serves as the key). The partial file name is the suffix for which
    timeseries files produced from that particular collection of history
    files will be named from.

    This primiarly accomplishes two things:
        1. History files are appropriately grouped by differences in the
           file names and that these groups are carried over into the
           file names for the timeseries datasets.
        2. Multiple groups can exist together in the same directory. This
           is typically the case for model output that contains more than
           one average timestep.

    Parameters
    ----------
    paths : list(Path)
        List of Path objects that point to a large, unsorted, unfiltered list
        of history files.
    input_head_dir : Path
        Path for head directory of input directory structure for the history
        file locations stored in `path`, used to generate reflective directory
        structure under `output_head_dir`.
    output_head_dir : Path
        Path to head direcory that reflective subdirectories will be generated
        under for outputing timeseries files.
    dir_name_swaps: dict
        Dictionary containing keyword swaps for output directory structure.
        For example, ``hist`` may replace ``proc/tseries/``
        (e.g. {``hist``: ``proc/tseries/``}) (Default: {}).

    Returns
    -------
    timeseries_groups : dict
        Dictionary containing output templates as keys and the paths to their
        corresponding history files as values.
    """
    timeseries_groups = {}
    parent_directories = {}
    for path in paths:
        if path.parent in parent_directories:
            parent_directories[path.parent].append(path)
        else:
            parent_directories[path.parent] = [path]

    for parent in parent_directories:
        groups = solveForGroupsLeftToRight([path.name for path in parent_directories[parent]])
        conflicts = {}
        for group in groups:
            for comparable_group in groups:
                if group != comparable_group and group in comparable_group:
                    if group in conflicts:
                        conflicts[group].append(comparable_group)
                    else:
                        conflicts[group] = [comparable_group]
        group_to_paths = {group: [] for group in groups}
        for path in parent_directories[parent]:
            for group in groups:
                if group in str(path.name):
                    if group in conflicts:
                        conflict = False
                        for conflict_group in conflicts[group]:
                            if conflict_group in str(path.name):
                                conflict = True
                                break
                        if not conflict:
                            group_to_paths[group].append(path)
                    else:
                        group_to_paths[group].append(path)

        group_output_dir = generateReflectiveOutputDirectory(input_head_dir, output_head_dir, parent, swaps=dir_name_swaps)
        for group in group_to_paths:
            timeseries_groups[Path(group_output_dir + "/" + group)] = group_to_paths[group]
    return timeseries_groups


def isVariableAuxiliary(variable_meta: dict,
                        auxiliary_dims: list = ["nbnd", "chars", "string_length", "hist_interval"],
                        max_num_dims: int = 1,
                        primary_dims: list = ["time"]) -> bool:
    r"""Determines if a variable is auxiliary based on metadata.

    This function determins which variables are deemed ``auxiliary``
    and thus stored in every resulting timeseries file. They should
    be small variables, typically of one dimension. Coordinate variables
    should meet this criteria, but some descriptive variables may have
    additional dimensions that are small in size.

    For now, these variables are identified by containing any of the dimensions
    specified in `auxiliary_dims`.

    Some variables are also small enough to fit in every timeseries file
    because they do not evolve with time (do not contain the time dimension).
    Variables that do not contain dimensions in `auxiliary_dims` will still
    be considered auxiliary if they do not any of the contain dimensions
    specified in `primary_dims`.

    The defaults are configured somewhat arbitrarily and will likely
    need to be improved for comaptability with other model runs (and
    definitely for other models). We may also need to implement a new method
    for identifying auxiliary variables other than just dimensions.

    Parameters
    ----------
    variable_meta : dict
        Dictionary containing metadata produced by `getHistoryFileMetaData`
    auxiliary_dims : list
        List of dimension names that, regardless of size, if found in a
        variable will designate that variable as auxiliary
        (Default: ["nbnd", "chars", "string_length", "hist_interval"]).
    max_num_dims : int
        Maximum number of dimensions a variable can have before it is possibly
        considered as primary and thus not auxiliary (Default: 1).
    primary_dims : list
        List of dimension names that, if the variable exceeds the maximum
        number of dimensions, will designate the variable as primary and thus
        not auxiliary (Default: ["time"]).

    Returns
    -------
    bool
        True if the variable is auxiliary, false if it is not.
    """
    dims = np.unique(variable_meta["dimensions"])

    for tag in auxiliary_dims:
        if tag in dims:
            return True

    if len(dims) > max_num_dims:
        for tag in primary_dims:
            if tag in dims:
                return False

    return True


def getHistoryFileMetaData(hs_file_path: Path) -> dict:
    r"""Builds dictionary containing metadata information for history file.

    This function reads coordinate and attribute information in the netCDF
    file to obtian metadata quickly. No variable data is loaded, so this
    function call is relatively lightweight and easily parallelized.

    The metadata pulled is specific to the timeseries generation stack and
    stored loosely in a dictionary. This may be converted into a class of
    it's own in the future.

    Parameters
    ----------
    hs_file_path : Path
        Path to history file to pull metadata from.

    Returns
    -------
    meta : dict
        Dictionary containing useful metadata for specified history file.
    """
    meta = {}
    ds = netCDF4.Dataset(hs_file_path, mode="r")
    try:
        meta["file_size"] = getsize(hs_file_path)
        meta["variables"] = list(ds.variables)
        meta["global_attrs"] = {key: getattr(ds, key) for key in ds.ncattrs()}
        meta["variable_meta"] = {}
        if "time" in meta["variables"]:
            meta["time"] = cftime.num2date(ds["time"][:], units=ds["time"].units, calendar=ds["time"].calendar)
        else:
            meta["time"] = None

        for variable in meta["variables"]:
            meta["variable_meta"][variable] = {}
            if type(ds[variable]) is netCDF4._netCDF4._Variable:
                for key in ds[variable].ncattrs():
                    meta["variable_meta"][variable][key] = ds[variable].__getattr__(key)
            else:
                for key in ds[variable].ncattrs():
                    meta["variable_meta"][variable][key] = ds[variable].getncattr(key)

            meta["variable_meta"][variable]["dimensions"] = list(ds[variable].dimensions)
            meta["variable_meta"][variable]["data_type"] = ds[variable].dtype
            meta["variable_meta"][variable]["shape"] = ds[variable].shape

        meta["primary_variables"] = []
        meta["auxiliary_variables"] = []
        for variable in meta["variable_meta"]:
            if isVariableAuxiliary(meta["variable_meta"][variable]):
                meta["auxiliary_variables"].append(variable)
            else:
                meta["primary_variables"].append(variable)
    except Exception as e:
        meta["exception"] = e
    ds.close()
    return meta


def getYearSlices(years: list, slice_length: int) -> list:
    r"""Generates list of index tuples for slicing time array into year chunks.

    Given a list of years, this function will divide the list into chunks, with
    maximum length specified by `slice_length`. The chunking is configured
    to align boundary years to multiples of the slice length. In other words,
    the timeseries may begin and end with a slice chunk shorter than
    `slice length`. For example, if a timeseries begins in 2015 and ends in
    2092 is chunked by 10 years, the first chunk will be 2015-2019 and the last
    chunk will be 2090-2092. Everything between will be 10 years (2020-2029,
    2030-2039, etc).

    Parameters
    ----------
    years : list
        Path to history file to pull metadata from.
    slice_length : int
        Number of years per chunk to slice.

    Returns
    -------
    meta : dict
        Dictionary containing useful metadata for specified history file.
    """
    slices = []
    last_slice_yr = years[0]
    for index in range(len(years)):
        if years[index] % slice_length == 0 and years[index] != last_slice_yr:
            if len(slices) == 0:
                slices.append((0, index))
            else:
                slices.append((slices[-1][1], index))
            last_slice_yr = years[index]
    if len(slices) == 0:
        slices.append((0, index + 1))
    elif slices[-1][1] != index + 1:
        slices.append((slices[-1][1], index + 1))
    return slices


def generateTimeSeries(output_template: Path,
                       hf_paths: list,
                       auxiliary_variables: list,
                       primary_variables: list,
                       time_str_format: str,
                       compression_level: int = None,
                       compression_algo: str = "bzip2",
                       overwrite: bool = False,
                       debug_timing: bool = True,
                       version: str = "source") -> tuple:
    r"""Generates timeseries files from history files.

    This is the primary function for generating the timeseries files and
    controls most of the I/O processing. This function can operate on one
    timeseries file at a time and access history files independently,
    thus this function can be called in parallel across multiple workers.

    All history files specified in `hf_paths` are opened and aggregated along
    the time dimension. They are assumed to be of the same group. The
    ``primary_variables`` key in `metadata` specifies which variables in each
    of the history files should be aggregated. Each variable is read separately
    and computed within this function call. Therefore, this list should be
    short for each function call, therefore more calls can be made on other
    workers for greater throughput. However, this list can be greater than one
    variable if this process needs to be consolidated.

    The naming of each timeseries files is determined via the `output_template`
    and `time_str_format`.

    Parameters
    ----------
    output_template : Path
        Incomplete path with parent subdirectory to output timeseries file to
        and name to use as suffix in naming it.
    hf_paths : list
        List of paths pointing to the history files from which these timeseries
        files will be generated from.
    auxiliary_variables : list
        List of names for auxiliary variables to be included in all resulting
        timeseries files.
    primary_variables : list
        List of names for primary variables to generate individual timeseries
        files for.
    time_str_format : str
        Date format code used to convert the CFTime object to a string. This
        may vary for different timesteps and should be determined before
        calling this function.
    compression_level : int
        Compression level to apply to all variables. None or 0 indicates no
        compression (Default: None).
    compression_algo : str
        Which netCDF4 compression algorithm to use. See netcdf4-python
        documentation for available algorithms (Default: 'bzip2').
    overwrite : bool
        Whether or not to overwrite timeseries files if they already exist at
        the generated paths (Default: False).
    debug_timing : bool
        Includes the time to generate each resulting timeseries file in the
        output tuples (Default: True).
    version : str
        Software version being used, included as an attribute in the generated
        timeseries files. (Default: 'source')

    Returns
    -------
    ts_paths : list
        Path to each timeseries file generated.
    tuple (if debug_timing)
        Time to compute timeseries at each path in `ts_paths`.
    """
    debug_start_time = time()
    output_template.parent.mkdir(parents=True, exist_ok=True)

    auxiliary_ds = netCDF4.MFDataset(hf_paths, aggdim="time", exclude=primary_variables)
    auxiliary_ds.set_auto_mask(False)
    auxiliary_ds.set_auto_scale(False)
    auxiliary_ds.set_always_mask(False)

    time_start_str = cftime.num2date(auxiliary_ds["time"][0],
                                     units=auxiliary_ds["time"].units,
                                     calendar=auxiliary_ds["time"].calendar).strftime(time_str_format)
    time_end_str = cftime.num2date(auxiliary_ds["time"][-1],
                                   units=auxiliary_ds["time"].units,
                                   calendar=auxiliary_ds["time"].calendar).strftime(time_str_format)

    auxiliary_variable_data = {}
    for auxiliary_var in auxiliary_variables:
        attrs = {}
        for key in auxiliary_ds[auxiliary_var].ncattrs():
            if type(auxiliary_ds[auxiliary_var]) is netCDF4._netCDF4._Variable:
                attrs[key] = auxiliary_ds[auxiliary_var].__getattr__(key)
            else:
                attrs[key] = auxiliary_ds[auxiliary_var].getncattr(key)
        auxiliary_variable_data[auxiliary_var] = {
            "attrs": attrs,
            "dimensions": auxiliary_ds[auxiliary_var].dimensions,
            "shape": auxiliary_ds[auxiliary_var].shape,
            "dtype": auxiliary_ds[auxiliary_var].dtype,
            "data": auxiliary_ds[auxiliary_var][:],
        }
    auxiliary_ds.close()

    primary_ds = netCDF4.MFDataset(hf_paths, aggdim="time", exclude=auxiliary_variables)
    primary_ds.set_auto_mask(False)
    primary_ds.set_auto_scale(False)
    primary_ds.set_always_mask(False)

    complevel = compression_level
    compression = None
    if complevel > 0:
        compression = "bzip2"

    ts_paths = []
    for primary_var in primary_variables:
        ts_path = output_template.parent / f"{output_template.name}{primary_var}.{time_start_str}.{time_end_str}.nc"
        ts_paths.append(ts_path)
        if isfile(ts_path) and not overwrite:
            try:
                ds = netCDF4.Dataset(ts_path, mode="r")
                attrs = {key: getattr(ds, key) for key in ds.ncattrs()}
                ds.close()
                if "timeseries_process_complete" in attrs and attrs["timeseries_process"] == "complete":
                    continue
                else:
                    remove(ts_path)
            except OSError:
                remove(ts_path)
        elif isfile(ts_path) and overwrite:
            remove(ts_path)
        ts_ds = netCDF4.Dataset(ts_path, mode="w")
        for dim_index, dim in enumerate(primary_ds[primary_var].dimensions):
            if dim not in ts_ds.dimensions:
                if dim == "time":
                    ts_ds.createDimension(dim, None)
                else:
                    ts_ds.createDimension(dim, primary_ds[primary_var].shape[dim_index])

        var_data = ts_ds.createVariable(primary_var,
                                        primary_ds[primary_var].dtype,
                                        primary_ds[primary_var].dimensions, complevel=complevel, compression=compression)
        var_data.set_auto_mask(False)
        var_data.set_auto_scale(False)
        var_data.set_always_mask(False)

        attrs = {}
        if type(primary_ds[primary_var]) is netCDF4._netCDF4._Variable:
            for key in primary_ds[primary_var].ncattrs():
                attrs[key] = primary_ds[primary_var].__getattr__(key)
        else:
            for key in primary_ds[primary_var].ncattrs():
                attrs[key] = primary_ds[primary_var].getncattr(key)

        ts_ds[primary_var].setncatts(attrs)

        time_chunk_size = 1
        if len(primary_ds[primary_var].shape) > 0 and "time" in primary_ds[primary_var].dimensions:
            for i in range(0, primary_ds[primary_var].shape[0], time_chunk_size):
                if i + time_chunk_size > primary_ds[primary_var].shape[0]:
                    time_chunk_size = primary_ds[primary_var].shape[0] - i
                var_data[i:i + time_chunk_size] = primary_ds[primary_var][i:i + time_chunk_size]
        else:
            var_data[:] = primary_ds[primary_var][:]

        for auxiliary_var in auxiliary_variable_data:
            for dim_index, dim in enumerate(auxiliary_variable_data[auxiliary_var]["dimensions"]):
                if dim not in ts_ds.dimensions:
                    ts_ds.createDimension(dim, auxiliary_variable_data[auxiliary_var]["shape"][dim_index])
            aux_var_data = ts_ds.createVariable(auxiliary_var,
                                                auxiliary_variable_data[auxiliary_var]["dtype"],
                                                auxiliary_variable_data[auxiliary_var]["dimensions"])
            aux_var_data.set_auto_mask(False)
            aux_var_data.set_auto_scale(False)
            aux_var_data.set_always_mask(False)
            ts_ds[auxiliary_var].setncatts(auxiliary_variable_data[auxiliary_var]["attrs"])

            aux_var_data[:] = auxiliary_variable_data[auxiliary_var]["data"]
        ts_ds.setncatts({key: getattr(primary_ds, key) for key in primary_ds.ncattrs()} | {"timeseries_software_version": version, "timeseries_process": "complete"})
        ts_ds.close()

    if debug_timing:
        return (time() - debug_start_time, ts_paths)
    else:
        return ts_paths


class ModelOutputDatabase:
    r"""Database for centralizing all history-to-timeseries file operations.

    Attributes
    ----------
    None

    Methods
    -------
    getHistoryFileMetaData()
        Returns metadata for particulary history file.
    getTimeSeriesGroups()
        Returns list of paths to detected history files grouped.
    getHistoryFilePaths()
        Returns list of paths to detected history files.
    getTimeStepHours()
        Determines timestep for group history files.
    getTimeStepStr()
        Determines timestep label for group of history files.
    getTimeStepStrFormat()
        Determines date string format code based on timstep label.
    build()
        Builds database by reading each history file and storing metadata.
    run()
        Starts process for generating timeseries files.
    """

    def __init__(self,
                 hf_head_dir: str,
                 ts_head_dir: str,
                 dir_name_swaps: dict = {},
                 file_exclusions: list = [],
                 dir_exclusions: list = ["rest", "logs"],
                 timeseries_year_length: int = 10,
                 overwrite: bool = False,
                 include_variables: list = None,
                 exclude_variables: list = None,
                 year_start: int = None,
                 year_end: int = None,
                 compression_level: int = None,
                 compression_algo: str = "bzip2",
                 variable_compression_levels: dict = None) -> None:
        r"""Parameters
        ----------
        hf_head_dir : str
            Path to head directory to structure with subdirectories containing
            history files.
        ts_head_dir : str
            Path to head directory where structure reflecting `hf_head_dir`
            will be created and timeseries files will be written to.
        dir_name_swaps : dict
            Dictionary for swapping out keyword directory names in the
            structure under `hf_head_dir` (e.g. ``{"hist" : "proc/tseries"}``
            (Default: {}).
        file_exclusions : list
            File names containing any of the keywords in this list will be
            excluded from the database (Default: []).
        dir_exclusions : list
            Directory names containing any of the keywords in this list will be
            excluded from the database (Default: ['rest', 'logs']).
        timeseries_year_length : int
            Number of years each timeseries file should be chunked to using
            `getYearSlices` (Default: 10).
        overwrite : bool
            Whether or not to overwrite timeseries files if they already exist
            at the generated paths (Default: False).
        include_variables : list
            Variables to include in either creating individual timeseries files
            for adding as auxiliary variables (Default: None).
        exclude_variables : list
            Variables to exclude from either creating individual timeseries
            files for adding as auxiliary variables (Default: None).
        year_start : int
            Starting year for timeseries generation, must be later than first
            history file timestamp to have an effect (Default: None).
        year_end : int
            Ending year for timeseries generation, must be later than last
            history file timestamp to have an effect (Default: None).
        compression_level : int
            Compression level to pass to netCDF4 engine when generating
            timeseries files (Default: None).
        compression_algo : str
            Compression algorithm to pass to netCDF4 engine when generating
            timeseries files. See netCDF4-python documentation for available
            algorithms (Default: 'bzip2').
        variable_compression_levels : dict
            Compression levels to apply to specific variables. Variable name is
            key and the compression level is the value (Default: None).
        """
        self.log("Initializing...")
        self.__hf_head_dir = Path(hf_head_dir)
        self.__ts_head_dir = Path(ts_head_dir)
        self.__overwrite = overwrite
        self.__timeseries_year_length = timeseries_year_length
        self.__include_variables = include_variables
        self.__exclude_variables = exclude_variables
        if self.__exclude_variables is None:
            self.__exclude_variables = []
        self.__year_start = year_start
        self.__year_end = year_end
        self.__compression_level = compression_level
        self.__compression_algo = compression_algo
        if self.__compression_level is None:
            self.__compression_level = 0
        self.__variable_compression_levels = variable_compression_levels
        self.__total_size = 0
        self.__built = False

        start_t = time()
        self.log(f"Searching tree for netCDF files: '{hf_head_dir}'")
        self.__history_file_paths = []
        for path in sorted(self.__hf_head_dir.rglob("*.nc")):
            exclude = False
            for exclusion in file_exclusions:
                if exclusion in path.name:
                    exclude = True

            directory_names = [directory.name for directory in sorted(path.parents)]

            for exclusion in dir_exclusions:
                if exclusion in directory_names:
                    exclude = True

            if not exclude:
                self.__history_file_paths.append(path)
        self.log("\tDone.")
        start_t = time()
        self.log("Grouping history files... ")
        self.__timeseries_group_paths = generateTimeSeriesGroupPaths(self.__history_file_paths, hf_head_dir, ts_head_dir, dir_name_swaps=dir_name_swaps)
        self.log("Done.")

    def log(self, msg, end="\n"):
        print(msg, end=end)

    def getHistoryFileMetaData(self,
                               history_file_path: Path) -> dict:
        r"""Returns metadata for particulary history file.

        Metadata is generated by `getHistoryFileMetaData`.
        """
        return self.__history_file_metas[history_file_path]

    def getTimeSeriesGroups(self):
        r"""Returns list of paths to detected history files grouped.

        Groups are determined by `generateTimeSeriesGroupPaths`,
        based on which directories the history files are sotred in and
        substrings in the file names.
        """
        return self.__timeseries_group_paths

    def getHistoryFilePaths(self):
        r"""Returns list of paths to detected history files."""
        return self.__history_file_paths

    def getTimeStepHours(self,
                         hf_paths: list):
        r"""Determines timestep for group history files.

        Given a group of history files in a timeseries, this function
        determines what the timestep is in hours.

        Note that this function relies on the time coordinate obtained from
        metadata, not the file name.

        Parameters
        ----------
        hf_paths : list
            List of paths to history files.

        Returns
        -------
        float
            Timestep in hours.
        """
        times = []
        for path in hf_paths:
            hf_time = self.__history_file_metas[path]["time"]
            if len(hf_time) == 0:
                return 0
            elif len(hf_time) == 1:
                times.append(hf_time[0])
            else:
                times.append(hf_time[0])
                times.append(hf_time[1])
                break

        times.sort()
        if len(times) == 1:
            return 0.0
        else:
            return (times[1] - times[0]).total_seconds() / 60 / 60

    def getTimeStepStr(self,
                       hf_paths: list) -> str:
        r"""Determines timestep label for group of history files.

        Given a group of history files in a timeseries, this function
        determines what group label should be used for the parent directory.
        This is also used to determine how the time range is shown in the
        file names.

        The timestep is obtained from `ModelOutputDatabase.getTimeStepHours`

        Parameters
        ----------
        hf_paths : list
            List of paths to history files.

        Returns
        -------
        str
            Label for timestep
        """
        if "time_period_freq" in self.__history_file_metas[hf_paths[0]]["global_attrs"]:
            return self.__history_file_metas[hf_paths[0]]["global_attrs"]["time_period_freq"]

        dt_hrs = self.getTimeStepHours(hf_paths)
        if dt_hrs >= 24*364:
            return f"year_{int(np.ceil(dt_hrs / (24*365)))}"
        elif dt_hrs >= 24*28:
            return f"month_{int(np.ceil(dt_hrs / (24*31)))}"
        elif dt_hrs >= 24:
            return f"day_{int(np.ceil(dt_hrs / (24)))}"
        else:
            return f"hour_{int(np.ceil(dt_hrs))}"

    def getTimeStepStrFormat(self,
                             timestep_str: str) -> str:
        r"""Determines date string format code based on timstep label.

        This function determines how the date ranges for each timeseries
        dataset is shown in the file name.

        Parameters
        ----------
        timestep_str : str
            Label for timestep group (e.g. ``day_1``).

        Returns
        -------
        str
            Date string format code for converting CFTime object to string for
            file naming.
        """
        if "hour" in timestep_str:
            return "%Y-%m-%d-%H"
        elif "day" in timestep_str:
            return "%Y-%m-%d"
        elif "month" in timestep_str:
            return "%Y-%m"
        elif "year" in timestep_str:
            return "%Y"
        else:
            return "%Y-%m-%d-%H"

    def build(self,
              client: dask.distributed.Client = None) -> None:
        r"""Builds database by reading each history file and storing metadata.

        Should be called before `run()`

        This only reads metadata (no variable data is loaded) and is therefore
        lightweight. This function recursviely reads the directory structure
        starting at the head directory specified at class initialization.

        Although reading metadata is lightweight, latency can increase with
        a large number of files. Therefore, Dask is used to convert metadata
        calls into lazy delayed functions and execute on a cluster. Unless a
        client is specified, it will automatically detect the Dask global
        client if it exists. If neither exists, then it proceeds in serial.

        Parameters
        ----------
        client : dask.distributed.Client
            Client object to use for Dask parallelization.

        Returns
        -------
        None
        """
        start_bt = time()
        self.log("Starting Build.")
        if client is None:
            client = dask.distributed.client._get_global_client()

        self.__gen_ts_args_templates = []
        self.__gen_ts_args_hf_paths = []
        self.__gen_ts_args_auxiliary_vars = []
        self.__gen_ts_args_primary_vars = []
        self.__gen_ts_args_time_formats = []
        self.__gen_ts_args_comp_levels = []
        self.__gen_ts_args_overwrites = []

        start_t = time()
        self.__history_file_metas = {}
        if client is None:
            self.log("Dask client not detected, proceeding serially.")
            self.log("\tGathering metadata...")
            for path in self.__history_file_paths:
                self.__history_file_metas[path] = getHistoryFileMetaData(path)
            self.log("\t Done.")
        else:
            self.log("Dask client detected.")
            self.log("\tGathering metadata...")
            bag = db.from_sequence(self.__history_file_paths, npartitions=20000).map(getHistoryFileMetaData)
            metas = bag.compute()
            for index, path in enumerate(self.__history_file_paths):
                self.__history_file_metas[path] = metas[index]
            self.log("\tDone.")

        start_t = time()
        self.log("\tComputing timeseries arguments...")
        new_timeseries_group_paths = {}
        for path_template in self.__timeseries_group_paths:
            hs_file_paths = self.__timeseries_group_paths[path_template]
            timestep_str = self.getTimeStepStr(hs_file_paths)
            new_path_template = (path_template.parent / timestep_str) / path_template.name
            new_timeseries_group_paths[new_path_template] = hs_file_paths
        self.__timeseries_group_paths = new_timeseries_group_paths

        for meta in metas:
            self.__total_size += meta["file_size"]

        self.__generateTimeSeries_args = []
        for output_template in self.getTimeSeriesGroups():
            hf_paths = self.getTimeSeriesGroups()[output_template]
            years = [self.getHistoryFileMetaData(hf_path)["time"][0].year for hf_path in hf_paths]

            for start_index, end_index in getYearSlices(years, self.__timeseries_year_length):
                within_range = True
                if self.__year_start is not None and years[start_index] < self.__year_start:
                    within_range = False
                    for index in range(start_index, end_index):
                        if years[start_index] >= self.__year_start:
                            start_index = index
                            within_range = True
                            break
                if self.__year_end is not None and years[end_index-1] > self.__year_end:
                    within_range = False
                    for index in range(start_index, end_index):
                        if years[end_index-1] <= self.__year_end:
                            end_index = index
                            within_range = True
                            break

                if not within_range or end_index <= start_index:
                    continue

                slice_paths = hf_paths[start_index:end_index]
                metadata = self.getHistoryFileMetaData(slice_paths[0])
                time_str_format = self.getTimeStepStrFormat(self.getTimeStepStr(slice_paths))

                for primary_variable in metadata["primary_variables"]:
                    if self.__include_variables is not None and primary_variable not in self.__include_variables:
                        continue
                    if primary_variable not in self.__exclude_variables:
                        primary_variables = np.array([primary_variable])
                        auxiliary_variables = np.array(metadata["auxiliary_variables"])
                        if self.__variable_compression_levels is not None and primary_variable in self.__variable_compression_levels:
                            compression_level = self.__variable_compression_levels[primary_variable]
                        else:
                            compression_level = self.__compression_level
                        self.__gen_ts_args_templates.append(output_template)
                        self.__gen_ts_args_hf_paths.append(slice_paths)
                        self.__gen_ts_args_auxiliary_vars.append(auxiliary_variables)
                        self.__gen_ts_args_primary_vars.append(primary_variables)
                        self.__gen_ts_args_time_formats.append(time_str_format)
                        self.__gen_ts_args_comp_levels.append(compression_level)
        self.log("\tDone.")
        self.log("Build complete.")
        self.__built = True

    def getGenTSArgs(self):
        return self.__generateTimeSeries_args

    def run(self,
            client: dask.distributed.Client = None,
            serial: bool = False,
            debug_timing: bool = False):
        r"""Starts process for generating timeseries files.

        This function generates the appropriate parameters for the database and
        parallelizes `generateTimeSeries` using the Dask cluster if it exists
        or runs in serial. The call is blocking and will wait for all
        timeseries files to be written to disk before returning the paths.

        Parameters
        ----------
        client : dask.distributed.Client
            Client object to use for Dask parallelization.
        serial : bool
            Whether or not to force serial execution and not use Dask
            (Default: False).

        Returns
        -------
        ts_paths : list
            List of paths pointing to timeseries files generated.
        """
        if not self.__built:
            self.log("Building arguments.")
            self.build(client=client)
        else:
            self.log("Build already complete. Skipping build step.")

        if client is None:
            client = dask.distributed.client._get_global_client()

        try:
            version = getVersion("gents")
        except PackageNotFoundError:
            self.log("No metadata found for package 'gents', assuming source installation.")
            version = "source"

        ts_paths = []
        if client is None or serial:
            self.log("ERROR: No Dask client detected, serial run() not yet implemented.")
            pass
        else:
            self.log("Dask client detected, mapping arguments to generateTimeSeries() in parallel.")
            futures = client.map(generateTimeSeries,
                                 self.__gen_ts_args_templates,
                                 self.__gen_ts_args_hf_paths,
                                 self.__gen_ts_args_auxiliary_vars,
                                 self.__gen_ts_args_primary_vars,
                                 self.__gen_ts_args_time_formats,
                                 self.__gen_ts_args_comp_levels,
                                 [self.__compression_algo]*len(self.__gen_ts_args_templates),
                                 [self.__overwrite]*len(self.__gen_ts_args_templates),
                                 [debug_timing]*len(self.__gen_ts_args_templates),
                                 [version]*len(self.__gen_ts_args_templates))
            self.log("Map complete, awaiting cluster computation...")
            ts_paths = client.gather(futures, errors="skip")
            self.log(f"\tDone.")
        return ts_paths