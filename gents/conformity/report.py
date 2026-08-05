"""
Result collection for conformity runs.

A conformity check is not an assertion: a run evaluates every check it can and
reports how many passed. A check **passes** when it was evaluated and held,
**fails** when it was evaluated and did not, and **skips** when it could not be
evaluated at all (nothing in the case for it to look at, or no input directory
given). Skips are excluded from the pass percentage rather than counted as
passes, so a run over a narrow case cannot inflate its own score.

Everything a researcher would read or edit lives in the per-model specifications
under ``gents/conformity/models/``; this module is only the machinery.
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

    :param model: Model specification applied, e.g. ``'CESM3'``.
    :type model: str
    :param spec_version: That specification's ``SPEC_VERSION``, recorded so a
        result stays interpretable as the specification gains checks.
    :type spec_version: int
    :param ts_dir: Time series output directory that was inspected.
    :type ts_dir: str
    :param hf_dir: History file directory the output came from, if available.
    :type hf_dir: str or None
    """

    model: str
    spec_version: int
    ts_dir: str
    hf_dir: str = None
    results: list = field(default_factory=list)

    def check(self, description, passed, detail=""):
        """
        Records a single pass/fail check.

        :param description: One-line statement of what must be true, in the
            model's own vocabulary. This text *is* the specification: it appears
            verbatim in the report.
        :type description: str
        :param passed: Whether the condition held.
        :type passed: bool
        :param detail: Optional context shown beneath the result.
        :type detail: str
        :returns: ``passed``, so callers can branch on it.
        :rtype: bool
        """
        self.results.append(
            CheckResult(description, PASS if passed else FAIL, detail)
        )
        return passed

    def check_each(self, description, items, predicate, name=str, max_examples=5):
        """
        Records one check covering many items, naming the ones that failed.

        Most conformity checks read "every output file must X", where the useful
        failure message is *which* files did not. One result per check, not per
        file, keeps the pass percentage a measure of coverage rather than of case
        size. Empty ``items`` skips rather than vacuously passing.

        :param description: One-line statement of what must be true of every item.
        :type description: str
        :param items: Items to test (consumed into a list).
        :type items: iterable
        :param predicate: Callable returning ``True`` if an item conforms.
        :type predicate: callable
        :param name: Callable rendering an item for the failure message.
        :type name: callable
        :param max_examples: Maximum offenders to name before summarising.
        :type max_examples: int
        :returns: ``True`` if every item conformed.
        :rtype: bool
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
        Records a check that could not be evaluated.

        :param description: The check that was not run.
        :type description: str
        :param reason: Why it could not be evaluated.
        :type reason: str
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
        Renders the run as a plain dictionary for JSON output.

        Carries enough provenance to interpret later: which model specification,
        which version of it, and which GenTS produced the output.
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
        """Renders the run as a human-readable console report."""
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
