import numpy as np
from pathlib import Path
import xarray


class GenerationConfig:
    def __build_groups(self, hist_dir_path, date_index, delimiter="."):
        paths = [path for path in hist_dir_path.iterdir()]
        files = [path.name for path in hist_dir_path.iterdir()]
        files_parsed = np.array([file.split(delimiter) for file in files])

        if date_index < 0:
            date_index = files_parsed.shape[1] + date_index

        prefix = ""
        sequence_identifiers = []
        for sequence_index in range(files_parsed.shape[1]):
            uniques = np.unique(files_parsed[:, sequence_index])
            if sequence_index != date_index and uniques.size > 1:
                sequence_identifiers.append((sequence_index, uniques))
            elif uniques.size == 1:
                if uniques[0] != "nc":
                    prefix += uniques[0] + delimiter

        groups = {}
        for index in range(len(files)):
            group_identifier = prefix
            for sequence_index, identifier in sequence_identifiers:
                group_identifier += files_parsed[index][sequence_index]
            if group_identifier in groups:
                groups[group_identifier].append(paths[index])
            else:
                groups[group_identifier] = [paths[index]]

        return groups

    def __init__(self, case_dir_paths, output_timeseries_path):
        self.input_case_dir_paths = [Path(str(path)) for path in case_dir_paths]
        for case_path in self.input_case_dir_paths:
            assert case_path.exists()
            assert not case_path.is_file()

        self.output_dir_path = Path(str(output_timeseries_path))
        assert self.output_dir_path.exists()
        assert not self.output_dir_path.is_file()

        self.output_timeseries_path = output_timeseries_path
        self.case_names = [path.name for path in self.input_case_dir_paths]
        self.possible_components = [
            "atm", "ocn", "lnd", "esp", "glc", "rof", "wav", "ice"
        ]
        self.history_dir_name = "hist"

        self.case_comp_hist_dir_paths = {}
        for case in self.input_case_dir_paths:
            comp_paths = {}
            for comp_path in case.iterdir():
                if comp_path.name in self.possible_components:
                    for sub_directory in comp_path.iterdir():
                        if sub_directory.name == self.history_dir_name:
                            comp_paths[sub_directory] = self.__build_groups(sub_directory, date_index=-2)
                            break
            self.case_comp_hist_dir_paths[case] = comp_paths

        print("Sampling dataset metadata from each case group to estimate total size in memory (this may take some time)...")
        self.group_nbytes = {}
        for case_dir in self.case_comp_hist_dir_paths:
            self.group_nbytes[case_dir] = {}
            for component_dir in self.case_comp_hist_dir_paths[case_dir]:
                self.group_nbytes[case_dir][component_dir] = {}
                for group in self.case_comp_hist_dir_paths[case_dir][component_dir]:
                    paths = self.case_comp_hist_dir_paths[case_dir][component_dir][group]
                    sample_ds = xarray.open_dataset(paths[0], chunks=dict(time=-1))
                    self.group_nbytes[case_dir][component_dir][group] = int(sample_ds.nbytes) * len(paths)
                    del sample_ds

    def fit_interm_timeseries_to_memory(self, memory_per_node_gb=150):
        interm_sizes = {}
        for case_dir in self.group_nbytes:
            interm_sizes[case_dir] = {}
            for component_dir in self.group_nbytes[case_dir]:
                interm_sizes[case_dir][component_dir] = {}
                for group in self.group_nbytes[case_dir][component_dir]:
                    total_size = (self.group_nbytes[case_dir][component_dir][group] / 1024**3)
                    num_files = len(self.case_comp_hist_dir_paths[case_dir][component_dir][group])
                    interm_sizes[case_dir][component_dir][group] = int(min(num_files / (total_size / memory_per_node_gb), num_files))
        return interm_sizes

    def get_timeseries_batches(self, interm_sizes):
        batches = []
        for case_dir in self.case_comp_hist_dir_paths:
            for component_dir in self.case_comp_hist_dir_paths[case_dir]:
                for group in self.case_comp_hist_dir_paths[case_dir][component_dir]:
                    output_dir = str(self.output_timeseries_path) + "/" + case_dir.name + str(component_dir).split(str(case_dir))[1]
                    output_dir = Path(output_dir.replace(self.history_dir_name, "tseries"))
                    file_paths = np.array(self.case_comp_hist_dir_paths[case_dir][component_dir][group])
                    interm_size = interm_sizes[case_dir][component_dir][group]
                    for batch_paths in np.array_split(file_paths, np.ceil(file_paths.size / interm_size)):
                        batches.append((output_dir, group, batch_paths))

        batches.sort(key=lambda entry: len(entry[2]))
        return batches