"""
``gents_conform`` -- run a model's conformity specification against generated output.

Conformity testing asks a different question from the unit tests: not "is this
function correct" but "did GenTS handle *this model's* output the way this
model's users need". It is therefore model-specific by design. A result is a
claim about a named model and case, which is what makes the documented results
table meaningful to a researcher deciding whether to trust GenTS with their run.

This is the same command CI runs over shareable sample cases and that a
researcher runs locally against a real case, so a documented result and a CI
result mean exactly the same thing.
"""

import argparse
import json
import sys
from pathlib import Path

from gents.conformity.models import SPECIFICATIONS
from gents.conformity.report import Report
from gents.utils import get_version


def parse_arguments():
    """
    Parse command-line arguments for the ``gents_conform`` entry point.

    :returns: Namespace populated with parsed argument values.
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser(
        description="Check GenTS time series output against a model's conformity "
                    "specification."
    )
    parser.add_argument(
        "ts_dir",
        type=str,
        nargs="?",
        help="Path to the head directory of the generated time series output."
    )
    parser.add_argument(
        "-i", "--hf_dir",
        type=str,
        default=None,
        help="Path to the head directory of the history files the output was "
             "generated from. Enables checks that compare output against its "
             "source; those checks are skipped when it is omitted."
    )
    parser.add_argument(
        "-m", "--model",
        type=str,
        default=None,
        help=f"Model specification to apply: {', '.join(sorted(SPECIFICATIONS))}."
    )
    parser.add_argument(
        "-V", "--version",
        action="version",
        version=f"%(prog)s {get_version()}"
    )
    parser.add_argument(
        "--list-models",
        action="store_true",
        help="List available model specifications and their versions, then exit."
    )
    parser.add_argument(
        "--json",
        dest="json_path",
        type=str,
        default=None,
        help="Also write the report as JSON to this path, for recording a result "
             "in the documented conformity table."
    )
    return parser.parse_args()


def main():
    """
    Entry point for the ``gents_conform`` command-line interface.

    Applies the selected model's conformity specification to a generated time
    series tree, prints a report, and exits non-zero if any check failed. A
    failed check is a finding to investigate, not a crash: every check that can
    be evaluated is evaluated, and the report shows the full picture.
    """
    args = parse_arguments()

    if args.list_models:
        print("Available conformity specifications:")
        for key, spec in sorted(SPECIFICATIONS.items()):
            print(f"  {key:<10} {spec.MODEL} (specification v{spec.SPEC_VERSION})")
        return 0

    if args.ts_dir is None:
        raise ValueError("No time series directory specified.")

    if args.model is None:
        raise ValueError(
            f"No model specified. Available: {', '.join(sorted(SPECIFICATIONS))}."
        )

    model_key = args.model.lower()
    if model_key not in SPECIFICATIONS:
        raise ValueError(
            f"No conformity specification available for model '{args.model}'. "
            f"Available: {', '.join(sorted(SPECIFICATIONS))}."
        )
    spec = SPECIFICATIONS[model_key]

    ts_dir = Path(args.ts_dir).resolve()
    if not ts_dir.is_dir():
        raise ValueError(f"Time series directory '{ts_dir}' does not exist.")

    hf_dir = None
    if args.hf_dir is not None:
        hf_dir = Path(args.hf_dir).resolve()
        if not hf_dir.is_dir():
            raise ValueError(f"History file directory '{hf_dir}' does not exist.")

    report = Report(
        model=spec.MODEL,
        spec_version=spec.SPEC_VERSION,
        ts_dir=str(ts_dir),
        hf_dir=str(hf_dir) if hf_dir else None,
    )
    spec.run(ts_dir, hf_dir, report)

    print(report.format_text())

    if args.json_path is not None:
        with open(args.json_path, "w") as file:
            json.dump(report.to_dict(), file, indent=2)
        print(f"  Report written to {args.json_path}\n")

    return 0 if report.conformant else 1


if __name__ == "__main__":
    sys.exit(main())
