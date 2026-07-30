"""
CESM3 conformity specification.

This file says what a correct GenTS run over a CESM3 case must produce. It is
written to be read by CESM3 researchers who may not write Python often: every
check is a plain loop and an if-statement, and each one explains in a comment
why CESM3 requires it. See ``README.md`` in the parent directory for how to run
these checks and how to audit or extend them.

The code repeats itself on purpose. Each check is written out in full rather
than shared with its neighbours, so that any single check can be read, changed,
or deleted on its own without having to understand the rest of the file. The one
exception is Section 3, where every check needs to look inside the files: opening
a netCDF file is slow enough that those checks share a single pass, while the
string tests elsewhere are fast enough that separate loops cost nothing.

**These expectations are written down by hand on purpose.** They are not read
out of ``gents/configs/gents_cesm3.yaml``. Checking that GenTS did what its own
configuration file told it to do would prove nothing about whether that
configuration is right for CESM3. The point is that a person wrote down what
CESM3 output must look like, independently. If someone changes the configuration
and does not change this file, a conformity run fails, and that is the check
doing its job.

Section 4 is the deliberate exception: it reads the include and exclude patterns
from the configuration file. There, the patterns are not what is being tested --
they are only how the section works out which streams to expect output from --
so reading them keeps the two lists from drifting apart.

Nothing here looks inside GenTS itself. Every check looks only at files on disk.
Whether GenTS parses its configuration correctly, or transposes data correctly,
is a question for the unit tests in ``gents/tests/``, not for this file.

The checks are grouped into four sections:

1. Directory structure -- where the files are.
2. File names -- what the files are called.
3. File contents -- what is inside the files.
4. Comparison against the original case -- did anything go missing.

**To add a check:** copy the nearest existing check, change it, and add one to
``SPEC_VERSION`` below. Nothing else needs to change.
"""

from fnmatch import fnmatch
from pathlib import Path

import yaml
from netCDF4 import Dataset

from gents.timeseries import check_timeseries_integrity, check_timeseries_conform

MODEL = "CESM3"

# Add one to this number whenever a check is added, removed, or changed. It is
# printed in every report, so that a recorded result like "CESM3 spec v1" still
# means something specific years later.
SPEC_VERSION = 1

# The longest a single time series file should be. Longer files are awkward to
# download and slow to open.
LONGEST_ALLOWED_SPAN_IN_YEARS = 10

# How many bad file names to print before summarising the rest.
HOW_MANY_EXAMPLES_TO_SHOW = 5


# ----------------------------------------------------------------------
# SMALL HELPERS
#
# These are used by the checks further down. Each one does exactly one
# simple thing to a file name.
# ----------------------------------------------------------------------


def describe_problems(bad_file_names, how_many_files_checked):
    """
    Build the one-line message printed underneath a check result.

    :param bad_file_names: Names of the files that failed the check.
    :param how_many_files_checked: How many files the check looked at in total.
    :returns: A sentence describing the outcome.
    """
    if len(bad_file_names) == 0:
        return f"{how_many_files_checked} files checked"

    message = f"{len(bad_file_names)} of {how_many_files_checked} files failed: "
    message = message + ", ".join(bad_file_names[:HOW_MANY_EXAMPLES_TO_SHOW])

    if len(bad_file_names) > HOW_MANY_EXAMPLES_TO_SHOW:
        how_many_are_hidden = len(bad_file_names) - HOW_MANY_EXAMPLES_TO_SHOW
        message = message + f" (and {how_many_are_hidden} more)"

    return message


def has_a_proper_name(ts_file):
    """
    Say whether a file is named the way CESM3 time series are supposed to be named.

    A correct name looks like this::

        b.e30_alpha09d.cam.h0a.ACTNI.000101-000612.nc
        |------- case -------|     |var| |-- dates --|

    That is: some case and stream information, then the variable name, then the
    range of dates the file covers, then ".nc".

    :param ts_file: The file to look at.
    :returns: True if the name follows the pattern, False otherwise.
    """
    if not ts_file.name.endswith(".nc"):
        return False

    # Chop off the ".nc" at the end, then split the rest on the dots.
    name_without_extension = ts_file.name[:-3]
    parts = name_without_extension.split(".")

    # We need at least a case name, a variable name, and a date range.
    if len(parts) < 3:
        return False

    # The date range is the last part, and looks like "000101-000612".
    date_range = parts[-1]
    halves = date_range.split("-")

    # There must be exactly two dates, joined by one dash.
    if len(halves) != 2:
        return False

    start_date = halves[0]
    end_date = halves[1]

    # Both dates must be made only of digits.
    if not start_date.isdigit():
        return False
    if not end_date.isdigit():
        return False

    # Both dates must be the same length. A range like "000101-0005" does not
    # say clearly whether it means months or years.
    if len(start_date) != len(end_date):
        return False

    return True


def get_variable_name(ts_file):
    """
    Pull the variable name out of a file name.

    For "b.e30_alpha09d.cam.h0a.ACTNI.000101-000612.nc" this returns "ACTNI".
    Only call this on a file that passed :func:`has_a_proper_name`.
    """
    name_without_extension = ts_file.name[:-3]
    parts = name_without_extension.split(".")
    return parts[-2]


def get_start_date(ts_file):
    """
    Pull the starting date out of a file name, as text.

    For "...ACTNI.000101-000612.nc" this returns "000101".
    Only call this on a file that passed :func:`has_a_proper_name`.
    """
    name_without_extension = ts_file.name[:-3]
    parts = name_without_extension.split(".")
    date_range = parts[-1]
    return date_range.split("-")[0]


def get_end_date(ts_file):
    """
    Pull the ending date out of a file name, as text.

    For "...ACTNI.000101-000612.nc" this returns "000612".
    Only call this on a file that passed :func:`has_a_proper_name`.
    """
    name_without_extension = ts_file.name[:-3]
    parts = name_without_extension.split(".")
    date_range = parts[-1]
    return date_range.split("-")[1]


def get_year(date_text):
    """
    Pull the year out of a date like "000612" or "00061231".

    The year is always the first four digits, whatever the rest of the date
    looks like. Returns it as a number, so "0006" becomes 6.
    """
    return int(date_text[:4])


def get_stream_name(history_file):
    """
    Pull the stream name out of a *history* file name.

    A CESM3 history file is named like::

        b.e30_alpha09d.cam.h0a.0001-06.nc

    Dropping the date at the end leaves "b.e30_alpha09d.cam.h0a", which
    identifies the stream. Every stream that GenTS is supposed to process should
    show up in the output under that same name.
    """
    name_without_extension = history_file.name[:-3]
    parts = name_without_extension.split(".")
    parts_without_the_date = parts[:-1]
    return ".".join(parts_without_the_date)


def get_stream_name_from_time_series(ts_file):
    """
    Pull the stream name out of a *time series* file name.

    A time series file has both a variable and a date range on the end, so two
    parts come off instead of one. For
    "b.e30_alpha09d.cam.h0a.ACTNI.000101-000612.nc" this returns
    "b.e30_alpha09d.cam.h0a", matching :func:`get_stream_name`.
    """
    name_without_extension = ts_file.name[:-3]
    parts = name_without_extension.split(".")
    parts_without_variable_and_date = parts[:-2]
    return ".".join(parts_without_variable_and_date)


def run(ts_dir, hf_dir, report):
    """
    Run every CESM3 conformity check against a folder of generated time series.

    Every check looks at every file. There is no option to check only some of
    them: a conformity result gets published as evidence that GenTS handled a
    case correctly, and a result taken from a handful of files cannot support
    that claim while still looking like it does. If a case is too big to check
    all the way through, the answer is a smaller case built on purpose, not a
    smaller sample of a big one.

    :param ts_dir: Folder holding the generated time series files.
    :type ts_dir: pathlib.Path
    :param hf_dir: Folder holding the original history files, or ``None`` if it
        is not available. The Section 4 checks are skipped when it is ``None``.
    :type hf_dir: pathlib.Path or None
    :param report: Object that collects the result of each check.
    :type report: gents.conformity.report.Report
    """
    # Find every time series file, anywhere underneath the output folder.
    ts_files = sorted(ts_dir.rglob("*.nc"))

    report.check(
        "Time series output was produced",
        len(ts_files) > 0,
        f"{len(ts_files)} files found under {ts_dir}",
    )

    # If nothing was produced there is nothing else to look at, and reporting a
    # score of zero out of zero would be misleading.
    if len(ts_files) == 0:
        return

    # ==================================================================
    # SECTION 1: DIRECTORY STRUCTURE
    #
    # Where the files ended up.
    # ==================================================================

    # CESM3 keeps processed output in a "proc/tseries" folder, next to the
    # "hist" folder holding the raw model output. Users and archiving tools
    # both rely on that to tell the two apart.
    bad_files = []
    for ts_file in ts_files:
        full_path = str(ts_file)
        if "proc/tseries" not in full_path:
            bad_files.append(ts_file.name)

    report.check(
        "Every time series file is inside a 'proc/tseries' folder",
        len(bad_files) == 0,
        describe_problems(bad_files, len(ts_files)),
    )

    # Time series must never be written back into the "hist" folder. That would
    # mix processed output in with the raw model output, and the next GenTS run
    # would start trying to process its own output.
    bad_files = []
    for ts_file in ts_files:
        full_path = str(ts_file)
        if "/hist/" in full_path:
            bad_files.append(ts_file.name)

    report.check(
        "No time series file is inside a 'hist' folder",
        len(bad_files) == 0,
        describe_problems(bad_files, len(ts_files)),
    )

    # Every file should sit in a folder naming how often the data was recorded,
    # such as "month_1" or "day_1". Without this, monthly and daily versions of
    # the same variable land in the same folder and cannot be told apart.
    bad_files = []
    for ts_file in ts_files:
        folder_name = ts_file.parent.name

        is_a_frequency_folder = False
        if folder_name.startswith("hour_"):
            is_a_frequency_folder = True
        if folder_name.startswith("day_"):
            is_a_frequency_folder = True
        if folder_name.startswith("month_"):
            is_a_frequency_folder = True
        if folder_name.startswith("year_"):
            is_a_frequency_folder = True

        if not is_a_frequency_folder:
            bad_files.append(f"{folder_name}/{ts_file.name}")

    report.check(
        "Every time series file is in a frequency folder such as 'month_1'",
        len(bad_files) == 0,
        describe_problems(bad_files, len(ts_files)),
    )

    # ==================================================================
    # SECTION 2: FILE NAMES
    #
    # What the files are called. The name is how people find a variable and
    # a date range without opening anything, so it has to be right.
    # ==================================================================

    # First, check that the names follow the pattern at all.
    bad_files = []
    for ts_file in ts_files:
        if not has_a_proper_name(ts_file):
            bad_files.append(ts_file.name)

    report.check(
        "Every file is named '<case>.<stream>.<variable>.<start>-<end>.nc'",
        len(bad_files) == 0,
        describe_problems(bad_files, len(ts_files)),
    )

    # The checks below read pieces out of the file names, so they can only be
    # done on files whose names follow the pattern. Collect those here.
    properly_named_files = []
    for ts_file in ts_files:
        if has_a_proper_name(ts_file):
            properly_named_files.append(ts_file)

    # Files should be split into chunks of at most ten years.
    bad_files = []
    for ts_file in properly_named_files:
        first_year = get_year(get_start_date(ts_file))
        last_year = get_year(get_end_date(ts_file))

        # A file covering years 1 through 10 spans ten years, so add one.
        how_many_years = last_year - first_year + 1

        if how_many_years > LONGEST_ALLOWED_SPAN_IN_YEARS:
            bad_files.append(ts_file.name)

    report.check(
        f"No time series covers more than {LONGEST_ALLOWED_SPAN_IN_YEARS} years",
        len(bad_files) == 0,
        describe_problems(bad_files, len(properly_named_files)),
    )

    # ---- Dates must be as precise as the data, and no more ----
    #
    # A monthly file dated "00010101" claims to know the day, which it does not.
    # Each frequency folder gets its own check below, so that a failure says
    # exactly which frequency went wrong.

    # Yearly data is dated like "0001", which is 4 digits.
    yearly_files = []
    for ts_file in properly_named_files:
        if ts_file.parent.name.startswith("year_"):
            yearly_files.append(ts_file)

    if len(yearly_files) == 0:
        report.skip(
            "Yearly files are dated with a 4-digit year, like '0001'",
            "no 'year_*' folders were found",
        )
    else:
        bad_files = []
        for ts_file in yearly_files:
            if len(get_start_date(ts_file)) != 4:
                bad_files.append(ts_file.name)

        report.check(
            "Yearly files are dated with a 4-digit year, like '0001'",
            len(bad_files) == 0,
            describe_problems(bad_files, len(yearly_files)),
        )

    # Monthly data is dated like "000106", which is 6 digits.
    monthly_files = []
    for ts_file in properly_named_files:
        if ts_file.parent.name.startswith("month_"):
            monthly_files.append(ts_file)

    if len(monthly_files) == 0:
        report.skip(
            "Monthly files are dated year-and-month, like '000106'",
            "no 'month_*' folders were found",
        )
    else:
        bad_files = []
        for ts_file in monthly_files:
            if len(get_start_date(ts_file)) != 6:
                bad_files.append(ts_file.name)

        report.check(
            "Monthly files are dated year-and-month, like '000106'",
            len(bad_files) == 0,
            describe_problems(bad_files, len(monthly_files)),
        )

    # Daily data is dated like "00010601", which is 8 digits.
    daily_files = []
    for ts_file in properly_named_files:
        if ts_file.parent.name.startswith("day_"):
            daily_files.append(ts_file)

    if len(daily_files) == 0:
        report.skip(
            "Daily files are dated down to the day, like '00010601'",
            "no 'day_*' folders were found",
        )
    else:
        bad_files = []
        for ts_file in daily_files:
            if len(get_start_date(ts_file)) != 8:
                bad_files.append(ts_file.name)

        report.check(
            "Daily files are dated down to the day, like '00010601'",
            len(bad_files) == 0,
            describe_problems(bad_files, len(daily_files)),
        )

    # Hourly data is dated like "0001060112", which is 10 digits. Data recorded
    # more often than once an hour needs seconds too, giving 14 digits.
    hourly_files = []
    for ts_file in properly_named_files:
        if ts_file.parent.name.startswith("hour_"):
            hourly_files.append(ts_file)

    if len(hourly_files) == 0:
        report.skip(
            "Hourly files are dated down to the hour, like '0001060112'",
            "no 'hour_*' folders were found",
        )
    else:
        bad_files = []
        for ts_file in hourly_files:
            how_many_digits = len(get_start_date(ts_file))
            if how_many_digits != 10 and how_many_digits != 14:
                bad_files.append(ts_file.name)

        report.check(
            "Hourly files are dated down to the hour, like '0001060112'",
            len(bad_files) == 0,
            describe_problems(bad_files, len(hourly_files)),
        )

    # ---- Files that should never have been processed ----
    #
    # Each of these would turn into a time series that looks perfectly normal
    # but means nothing, which is worse than producing no file at all. Each one
    # is written out separately so a failure names the exact kind of file.

    # ".cam.i." files are snapshots of the model's starting state, not history.
    bad_files = []
    for ts_file in ts_files:
        if ".cam.i." in str(ts_file):
            bad_files.append(ts_file.name)

    report.check(
        "No time series was made from CAM instantaneous ('.cam.i.') files",
        len(bad_files) == 0,
        describe_problems(bad_files, len(ts_files)),
    )

    # Restart files hold the model's internal state so a run can resume.
    bad_files = []
    for ts_file in ts_files:
        if "/rest/" in str(ts_file):
            bad_files.append(ts_file.name)

    report.check(
        "No time series was made from restart files",
        len(bad_files) == 0,
        describe_problems(bad_files, len(ts_files)),
    )

    # Log files record what the run did; they are not scientific output.
    bad_files = []
    for ts_file in ts_files:
        if "/logs/" in str(ts_file):
            bad_files.append(ts_file.name)

    report.check(
        "No time series was made from log files",
        len(bad_files) == 0,
        describe_problems(bad_files, len(ts_files)),
    )

    # The MOM6 ocean grid never changes, so a time series of it is pointless.
    bad_files = []
    for ts_file in ts_files:
        if "ocean_geometry" in str(ts_file):
            bad_files.append(ts_file.name)

    report.check(
        "No time series was made from the MOM6 ocean grid file",
        len(bad_files) == 0,
        describe_problems(bad_files, len(ts_files)),
    )

    # MOM6 initial conditions describe the ocean at the start, not over time.
    bad_files = []
    for ts_file in ts_files:
        if "mom6.ic." in str(ts_file):
            bad_files.append(ts_file.name)

    report.check(
        "No time series was made from MOM6 initial condition files",
        len(bad_files) == 0,
        describe_problems(bad_files, len(ts_files)),
    )

    # Static files hold values that do not change with time, by definition.
    bad_files = []
    for ts_file in ts_files:
        if ".static." in str(ts_file):
            bad_files.append(ts_file.name)

    report.check(
        "No time series was made from static files",
        len(bad_files) == 0,
        describe_problems(bad_files, len(ts_files)),
    )

    # POP horizontal velocity grid files describe the grid, not the ocean.
    bad_files = []
    for ts_file in ts_files:
        if ".pop.hv." in str(ts_file):
            bad_files.append(ts_file.name)

    report.check(
        "No time series was made from POP grid files",
        len(bad_files) == 0,
        describe_problems(bad_files, len(ts_files)),
    )

    # Files ending in "tmp.nc" are scratch files left behind by the run.
    bad_files = []
    for ts_file in ts_files:
        if ts_file.name.endswith("tmp.nc"):
            bad_files.append(ts_file.name)

    report.check(
        "No time series was made from temporary files",
        len(bad_files) == 0,
        describe_problems(bad_files, len(ts_files)),
    )

    # An initial history file is a single snapshot, so there is no series in it.
    bad_files = []
    for ts_file in ts_files:
        if "initial_hist" in str(ts_file):
            bad_files.append(ts_file.name)

    report.check(
        "No time series was made from initial history files",
        len(bad_files) == 0,
        describe_problems(bad_files, len(ts_files)),
    )

    # ==================================================================
    # SECTION 3: FILE CONTENTS
    #
    # What is actually inside the files.
    # ==================================================================

    # GenTS writes its version number into a file only after the file is
    # finished. A file without one was left half-written when something went
    # wrong, and would quietly hand an analyst incomplete data.
    bad_files = []
    for ts_file in ts_files:
        if not check_timeseries_integrity(str(ts_file)):
            bad_files.append(ts_file.name)

    report.check(
        "Every time series file is complete and can be opened",
        len(bad_files) == 0,
        describe_problems(bad_files, len(ts_files)),
    )

    # Time series exist to be read along the time axis. How the data is laid out
    # on disk (its "chunking") decides whether that is fast or painfully slow.
    bad_files = []
    for ts_file in ts_files:
        if not check_timeseries_conform(str(ts_file)):
            bad_files.append(ts_file.name)

    report.check(
        "Every time series file is laid out for fast reading along time",
        len(bad_files) == 0,
        describe_problems(bad_files, len(ts_files)),
    )

    # The remaining checks all need to look inside the files. Open each file
    # once, and note down anything wrong, so that a big case is only read
    # through one time.
    files_missing_their_variable = []
    files_missing_the_command = []
    files_with_out_of_order_time = []
    files_with_wrong_compression = []
    how_many_files_were_opened = 0
    how_many_files_had_compression = 0

    for ts_file in properly_named_files:
        variable_name = get_variable_name(ts_file)

        # A file that will not open was already reported above, so skip it here
        # rather than reporting the same broken file over and over.
        try:
            dataset = Dataset(str(ts_file), "r")
        except OSError:
            continue

        how_many_files_were_opened = how_many_files_were_opened + 1

        # GenTS records the exact command that made the file. Without it, nobody
        # can reproduce the file months later.
        if "gents_command" not in dataset.ncattrs():
            files_missing_the_command.append(ts_file.name)

        # The data should be compressed with zlib at level 2. That is the
        # setting CESM3 picked as a balance between file size and how long
        # files take to read.
        if variable_name in dataset.variables:
            how_many_files_had_compression = how_many_files_had_compression + 1

            compression_settings = dataset[variable_name].filters()
            uses_zlib = compression_settings.get("zlib")
            compression_level = compression_settings.get("complevel")

            if uses_zlib is not True or compression_level != 2:
                files_with_wrong_compression.append(
                    f"{ts_file.name} (zlib={uses_zlib}, level={compression_level})"
                )

        dataset.close()

    report.check(
        "Every file records the GenTS command that made it",
        len(files_missing_the_command) == 0,
        describe_problems(files_missing_the_command, how_many_files_were_opened),
    )

    report.check(
        "Every variable is compressed with zlib at level 2",
        len(files_with_wrong_compression) == 0,
        describe_problems(files_with_wrong_compression, how_many_files_had_compression),
    )

    # ==================================================================
    # SECTION 4: COMPARISON AGAINST THE ORIGINAL CASE
    #
    # Everything above looks only at the output. This section compares the
    # output against the history files it came from, to catch things that
    # are missing entirely.
    # ==================================================================

    if hf_dir is None:
        report.skip(
            "Every history stream that should be processed produced output",
            "the history file folder was not provided",
        )
        return

    history_files = sorted(hf_dir.rglob("*.nc"))

    # Open the CESM3 configuration file that GenTS itself uses. It lives at
    # gents/configs/gents_cesm3.yaml, three folders up from this one.
    config_path = Path(__file__).parent.parent.parent / "configs" / "gents_cesm3.yaml"
    with open(config_path, "r") as config_file:
        config = yaml.safe_load(config_file)

    # Work out which streams exist in the output already.
    streams_found_in_the_output = []
    for ts_file in properly_named_files:
        stream_name = get_stream_name_from_time_series(ts_file)
        if stream_name not in streams_found_in_the_output:
            streams_found_in_the_output.append(stream_name)

    # To work out which streams should have produced output, we need to know
    # which history files GenTS was told to leave alone. Those patterns are read
    # straight out of the CESM3 configuration file instead of being written out
    # again here.
    #
    # Reading the configuration is the right thing to do here, and the wrong
    # thing to do in Section 2. Section 2 tests whether the exclusions actually
    # worked, so it has to say what they are independently -- checking the
    # configuration against itself would prove nothing. In this section the
    # exclusions are not what is being tested. They are only how we work out
    # which streams to expect, so reading them keeps the two lists from
    # drifting apart as the configuration changes.
    patterns_to_include = config["input_hf"]["include"]
    patterns_to_ignore = config["input_hf"]["exclude"]

    streams_expected_from_the_input = []
    for history_file in history_files:
        full_path = str(history_file)

        # A history file has to match at least one "include" pattern to be
        # picked up by GenTS in the first place.
        was_picked_up = False
        for pattern in patterns_to_include:
            if fnmatch(full_path, pattern):
                was_picked_up = True

        if not was_picked_up:
            continue

        # ...and must not match any "exclude" pattern.
        should_be_skipped = False
        for pattern in patterns_to_ignore:
            if fnmatch(full_path, pattern):
                should_be_skipped = True

        if should_be_skipped:
            continue

        stream_name = get_stream_name(history_file)
        if stream_name not in streams_expected_from_the_input:
            streams_expected_from_the_input.append(stream_name)

    # A stream that quietly produced nothing is the easiest failure to miss,
    # because the output folder still looks full of files.
    missing_streams = []
    for stream_name in streams_expected_from_the_input:
        if stream_name not in streams_found_in_the_output:
            missing_streams.append(stream_name)

    report.check(
        "Every history stream that should be processed produced output",
        len(missing_streams) == 0,
        describe_problems(missing_streams, len(streams_expected_from_the_input)),
    )
