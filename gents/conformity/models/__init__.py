"""
Per-model conformity specifications.

Each module here states what correct GenTS output looks like for one model, in
that model's own terms, exposing:

- ``MODEL`` -- display name of the model, e.g. ``'CESM3'``.
- ``SPEC_VERSION`` -- integer bumped whenever the checks change.
- ``run(ts_dir, hf_dir, report)`` -- record results into ``report``.

Specifications are deliberately independent: CESM3's conventions are not E3SM's,
and a shared check layer would either paper over that or be parameterised until
it checked nothing specific. Duplication buys each specification being readable
and auditable on its own, and keeps coverage explicit -- an unverified model
shows as unverified rather than inheriting generic checks.

Every check covers every file. There is no facility for checking a subset,
because a result drawn from a sample cannot support the claim it appears to make;
cases stay checkable by being designed small, not by being sampled.

**House style.** These modules are read and edited by researchers, so they are
written plainly on purpose: explicit ``for`` and ``if``, no lambdas,
comprehensions or table-driven dispatch, each check spelled out beside a comment
saying why the model requires it. Any one check should be readable and removable
without understanding its neighbours. The exception is expensive work: checks
that must open files share one pass, while cheap string checks each keep their
own loop (merging those saved 0.6 ms against 1442 ms of file opening on a
1224-file case).

To add a model, write the module and register it in ``SPECIFICATIONS`` below.
"""

from gents.conformity.models import cesm3

# Keys are the values accepted by ``gents_conform --model``, matched
# case-insensitively. Mirrors ``model_config_files`` in ``gents.cli``.
SPECIFICATIONS = {
    "cesm3": cesm3,
}
