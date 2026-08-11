# Conformity Testing

Conformity testing answers a different question from the unit tests, and the two
should not be mixed.

| | Unit tests (`gents/tests/`) | Conformity tests (`gents/conformity/`) |
|---|---|---|
| **Question** | Is this function correct? | Did GenTS handle *this model's* output the way this model's users need? |
| **Input** | Small synthetic netCDF fixtures built in code | A real model case, cloned down to a testable size |
| **Looks at** | GenTS internals | Only the files GenTS wrote to disk |
| **A failure means** | The code is broken | GenTS mishandled a case |
| **Audience** | Developers | Researchers and developers |
| **Run by** | `pytest gents/tests/` | `gents_conform` |

Whether `gents_cesm3.yaml` is parsed correctly is a **unit test** concern, but whether the resulting CESM3 output is formatted in the specific way a CESM3 researcher needs is a **conformity** concern.

Conformity is deliberately **model-specific**. There is no shared, model-agnostic check layer to avoid making assumptions about what each modeling center expects. A researcher deciding whether to trust GenTS with their run needs to know that it has been verified against a similar case for *their specific model*.

## Layout

```
gents/conformity/
├── README.md
├── case_builder.py    builds testable clones of real cases (gents_conform_build)
├── check.py           runs a specification against output (gents_conform)
├── report.py          collects results, prints them, writes JSON
└── models/
    ├── __init__.py    registry of available specifications
    └── cesm3.py       what correct CESM3 output looks like
```

## Running the workflow

Conformity checks can be run in three steps: build a testable NaN-filled case, generate time series from it, and then check the result using a model spec.

### 1. Build a testable case (once, offline)

Real cases are far too large to test against directly. `gents_conform_build`
mirrors a case into structurally identical clones whose scientific data has been
replaced with missing values. This makes them small enough to store online and share:

```bash
gents_conform_build /glade/derecho/scratch/me/my_case -o ./my_case_clone -n 16
```

Add `-d/--dryrun` to inspect a case before committing to the clone:
```
  Files to clone                  : 1383
  Files with time coordinates     : 1383
  Unique variables                : 820
  Output frequencies              : day_1, hour_3, month_1
  Years spanned                   : 1 - 7
Dry run: 1383 file(s) would be cloned.
```

`-v/--verbose` prints the same summary during a real build.

The clone keeps every dimension, attribute, coordinate and variable of the original, just without the actual data values. Conformity is about checking the structure, naming, and metadata, not scientific accuracy (GenTS never modifies data values, it only slices and transforms them).

The command that built the clone is recorded in a `cmd.txt` at the top of the clone directory, so a cloned case always carries the recipe for rebuilding it.

### 2. Generate time series

Run GenTS over the clone with `--no-data`. This builds the full output structure without reading or writing any primary data:

```bash
run_gents ./my_case_clone -o ./my_case_output --model CESM3 --no-data
```

`--no-data` is what makes `run_gents` fast enough to run in CI. It is safe here for the same reason the clone is: nothing in a conformity check reads scientific values.

> On a case with very wide streams, add `--memory-limit <GB>` to cap how much variable data each worker caches.

### 3. Check the output

```bash
gents_conform ./my_case_output -i ./my_case_clone --model CESM3
```

The optional `-i` argument enables checks that compare GenTS output against the case it was derived from. These checks can catch an output stream that silently produced nothing. Without it, those checks are reported as skipped.

The command exits `0` if every check passed and `1` if any failed. Add `--json report.json` to record the result. `--list-models` shows which specifications exist and their versions.

Output looks like this:

```
GenTS Conformity Report
  Model            : CESM3 (specification v1)
  GenTS version    : 1.1.3.dev27+g3d426fbe4
  History files    : /scratch/case/b.e30_alpha09d...
  Time series      : /scratch/out

  [PASS] Every time series file is inside a 'proc/tseries' folder
         1224 files checked
  [FAIL] Every time series file is in a frequency folder such as 'month_1'
         1224 of 1224 files failed: ...
  [SKIP] Daily files are dated down to the day, like '00010601'
         no 'day_*' folders were found

  18/20 checks passed (90%), 4 skipped
```

Three outcomes are possible. **PASS** and **FAIL** mean the check ran.
**SKIP** means it could not run. A skip is useful information in itself: the four skips above indicate that this case has no daily, hourly, yearly or sub-hourly streams.

## Auditing and changing a model specification

`models/cesm3.py` is written to be read by researchers who may not write Python often. Every check is a plain loop, an `if` statement, and a comment explaining why CESM3 requires it. If a check is not clear from reading it, that is a bug in the check.

The file is organised into four sections:

1. **Directory structure**
2. **File names**
3. **File contents**
4. **Comparison against the original case** - optional

### Reading a check

Every check has the same four-part shape:

```python
# Time series must never be written back into the "hist" folder. That would
# mix processed output in with the raw model output, and the next GenTS run
# would start trying to process its own output.
bad_files = []                            # 1. somewhere to collect problems
for ts_file in ts_files:                  # 2. look at every file
    if "/hist/" in str(ts_file):          # 3. is this one wrong?
        bad_files.append(ts_file.name)

report.check(                             # 4. record the result
    "No time series file is inside a 'hist' folder",
    len(bad_files) == 0,
    describe_problems(bad_files, len(ts_files)),
)
```

The text passed to `report.check` is the specification. It appears verbatim in the report and in the documentation table, so write it as a statement a researcher would recognise.

### Adding a check

1. Copy the nearest existing check and change it.
2. Write a comment saying **why the model requires it**, in the model's own vocabulary.
3. Add one to `SPEC_VERSION` at the top of the file.

Nothing else needs changing. Checks are deliberately independent, so adding,
editing or deleting one should never require understanding other checks.

### The rules the file follows

**Expectations are restated by hand, not read from the config.** A check must not derive what it expects from `gents/configs/gents_cesm3.yaml`. Asserting that GenTS did what its own configuration told it to do is circular. If the conformity checks fails after changes are made to the YAML configuration, then the check either needs to be updated or the YAML file needs to be fixed.

Section 4 reads the include and exclude patterns from the YAML, but they are not what is being expected: they are only how the section works out which streams should have produced output. In this case, the output streams having corresponding time series files is what is being checked, not which patterns are being included/excluded.

**Readability beats brevity.** Model python files favor explicit `for` loops and `if` statements, over lambdas, comprehensions, and lookup tables. Repetition
between checks is expected and welcomed: it is what lets each check be easily read and changed on its own.

## Adding a new model

1. Write `models/<model>.py` exposing `MODEL`, `SPEC_VERSION`, and
   `run(ts_dir, hf_dir, report)`.
2. Register it in `SPECIFICATIONS` in `models/__init__.py`.

Do not try to share checks with an existing specification. Start by copying the closest one and rewriting it for the new model's conventions. Duplication between specifications is deliberate: **each model specification has to stand on its own as a readable statement of what that users require.**
