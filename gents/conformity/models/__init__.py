"""
Per-model conformity specifications.

Each module here states what correct GenTS output looks like for one model, in
that model's own terms. Specifications are deliberately independent of one
another: CESM3's date-string and directory conventions are not E3SM's, and a
shared abstraction would either paper over those differences or have to be
parameterised until it no longer checks anything specific. Duplication between
specifications is accepted in exchange for each one being readable, and
auditable, on its own.

A specification module exposes:

- ``MODEL`` -- display name of the model (e.g. ``'CESM3'``).
- ``SPEC_VERSION`` -- integer bumped whenever the checks change.
- ``run(ts_dir, hf_dir, report)`` -- record results into ``report``.

Every check covers every file: there is no facility for checking a subset. A
conformity result is published as evidence, and evidence drawn from a sample
cannot support the claim the result appears to make. Cases are kept checkable by
being designed small and deliberate, not by being sampled.

**House style.** A specification is read and edited by researchers who may not
write Python often, so these modules are written plainly on purpose: explicit
``for`` loops and ``if`` statements, no lambdas or comprehensions, and no
table-driven dispatch. Each check is spelled out in full next to the comment
explaining why the model requires it, so that any one check can be read,
changed, or deleted without understanding its neighbours. Repetition between
checks is the price of that, and it is worth paying.

The one place a loop is shared between checks is where the per-item work is
expensive. Opening a netCDF file costs milliseconds, so all the checks that need
to look inside a file share a single pass; testing a substring against a path
already in memory costs nanoseconds, so those checks each keep their own loop.
Measured on a 1224-file case, merging the cheap loops saved 0.6 ms against 1442
ms spent opening files -- nothing worth trading readability for.

To add a model, write the module and register it in ``SPECIFICATIONS`` below.
"""

from gents.conformity.models import cesm3

# Keys are the values accepted by ``gents_conform --model``, matched
# case-insensitively. Mirrors ``model_config_files`` in ``gents.cli``.
SPECIFICATIONS = {
    "cesm3": cesm3,
}
