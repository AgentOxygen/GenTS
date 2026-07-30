"""
Result collection for conformity runs.

A conformity check is *not* an assertion: a run evaluates every check it can and
reports how many passed, so a researcher pointing this at a real case sees the
full picture rather than the first thing that went wrong. This module holds the
small amount of machinery that makes that possible. Everything a researcher
would want to read or edit lives in the per-model specifications under
``gents/conformity/models/``.

Three outcomes are possible:

- **pass** - the check was evaluated and held.
- **fail** - the check was evaluated and did not hold.
- **skip** - the check could not be evaluated (e.g. it needs the input case
  directory and only the output tree was given, or there was nothing to check).
  Skipped checks are excluded from the pass percentage and reported separately,
  so a run over a partial case never inflates its own score.
"""

from dataclasses import dataclass, field

PASS = "pass"
FAIL = "fail"
SKIP = "skip"


@dataclass
class CheckResult:
    """A single evaluated check."""

    description: str
    status: str
    detail: str = ""


@dataclass
class Report:
    """
    Accumulates :class:`CheckResult` entries for one conformity run.

    :param model: Name of the model specification applied (e.g. ``'CESM3'``).
    :param spec_version: Version of that specification (see the model module's
        ``SPEC_VERSION``). Recorded so a documented result stays meaningful as
        the specification gains checks.
    :param ts_dir: Time-series output directory that was inspected.
    :param hf_dir: History file directory the output was generated from, or
        ``None`` if it was not available.
    """

    model: str
    spec_version: int
    ts_dir: str
    hf_dir: str = None
    results: list = field(default_factory=list)

    def check(self, description, passed, detail=""):
        """
        Record a single pass/fail check.

        :param description: One-line statement of what must be true, in the
            model's own vocabulary. This text is the specification -- it appears
            verbatim in the report and in the documentation table.
        :param passed: Whether the condition held.
        :param detail: Optional context shown beneath the result.
        :returns: ``passed``, so callers can branch on it.
        """
        self.results.append(
            CheckResult(description, PASS if passed else FAIL, detail)
        )
        return passed

    def check_each(self, description, items, predicate, name=str, max_examples=5):
        """
        Record one check covering many items, naming the ones that failed.

        Most conformity checks are of the form "every output file must X", and
        the useful failure message is *which* files did not, not merely that
        some did not. An empty ``items`` is reported as a skip rather than a
        vacuous pass.

        :param description: One-line statement of what must be true of every item.
        :param items: Items to test (consumed into a list).
        :param predicate: Callable returning ``True`` if an item conforms.
        :param name: Callable rendering an item for the failure message.
        :param max_examples: Maximum offenders to name before summarising.
        :returns: ``True`` if every item conformed.
        """
        items = list(items)
        if not items:
            self.skip(description, "nothing to check")
            return False

        offenders = [name(item) for item in items if not predicate(item)]
        if not offenders:
            self.check(description, True, f"{len(items)} checked")
            return True

        shown = ", ".join(offenders[:max_examples])
        if len(offenders) > max_examples:
            shown += f" (+{len(offenders) - max_examples} more)"
        self.check(
            description, False, f"{len(offenders)} of {len(items)} failed: {shown}"
        )
        return False

    def skip(self, description, reason):
        """
        Record a check that could not be evaluated.

        :param description: The check that was not run.
        :param reason: Why it could not be evaluated.
        """
        self.results.append(CheckResult(description, SKIP, reason))
        return None

    def count(self, status):
        """Number of results with the given status."""
        return sum(1 for result in self.results if result.status == status)

    @property
    def evaluated(self):
        """Number of checks that were actually evaluated (pass + fail)."""
        return self.count(PASS) + self.count(FAIL)

    @property
    def pass_rate(self):
        """
        Fraction of evaluated checks that passed, in ``[0, 1]``.

        Skipped checks are excluded from both numerator and denominator.
        Returns ``0.0`` when nothing could be evaluated.
        """
        if self.evaluated == 0:
            return 0.0
        return self.count(PASS) / self.evaluated

    @property
    def conformant(self):
        """``True`` only if at least one check ran and none failed."""
        return self.evaluated > 0 and self.count(FAIL) == 0

    def to_dict(self):
        """
        Render the run as a plain dictionary for JSON output.

        This is the payload a researcher attaches to a documentation ledger
        entry, so it carries enough provenance to interpret later: which model
        specification, which version of it, and which GenTS produced the output.
        """
        from gents.utils import get_version

        return {
            "model": self.model,
            "spec_version": self.spec_version,
            "gents_version": get_version(),
            "hf_dir": self.hf_dir,
            "ts_dir": self.ts_dir,
            "passed": self.count(PASS),
            "failed": self.count(FAIL),
            "skipped": self.count(SKIP),
            "pass_rate": round(self.pass_rate, 4),
            "conformant": self.conformant,
            "checks": [
                {
                    "description": result.description,
                    "status": result.status,
                    "detail": result.detail,
                }
                for result in self.results
            ],
        }

    def format_text(self):
        """Render the run as a human-readable console report."""
        from gents.utils import get_version

        lines = [
            "",
            "GenTS Conformity Report",
            f"  Model            : {self.model} (specification v{self.spec_version})",
            f"  GenTS version    : {get_version()}",
            f"  History files    : {self.hf_dir or '(not provided)'}",
            f"  Time series      : {self.ts_dir}",
            "",
        ]

        marks = {PASS: "PASS", FAIL: "FAIL", SKIP: "SKIP"}
        for result in self.results:
            lines.append(f"  [{marks[result.status]}] {result.description}")
            if result.detail:
                lines.append(f"         {result.detail}")

        percent = round(100 * self.pass_rate)
        lines += [
            "",
            f"  {self.count(PASS)}/{self.evaluated} checks passed ({percent}%)"
            + (f", {self.count(SKIP)} skipped" if self.count(SKIP) else ""),
            "",
        ]
        return "\n".join(lines)
