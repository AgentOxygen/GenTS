Conformity
==========

While *unit* testing ensures that the GenTS API is functioning correctly, *conformity* testing evaluates how well the preset model configurations produce time series files that conform to each modeling center's expectations. For example, all time series files for `CESM3 <https://www.cesm.ucar.edu/models/cesm3>`_ are expected to span no more than 10 years. This is not a core GenTS expectation and therefore not a unit testing issue, so it is instead described explicitly in the conformity specification for CESM3.

Each case is a clone of a real simulation built with ``gents_conform_build``, in which every primary variable is replaced by an empty, missing-value array. The clone keeps the full directory layout, file names, dimensions, attributes, and metadata of the original, but uses significantly less disk space. The original command used to generate the cloned case is stored in a ``cmd.txt`` file in the top-level directory. GenTS is then run over the clone with ``--no-data``, and ``gents_conform`` checks the resulting time series tree against the model's specification in `gents/conformity/models/ <https://github.com/AgentOxygen/GenTS/tree/main/gents/conformity/models>`_.

Latest Test Results
-------------------

Each row below records one run of one case and links to the `GitHub Actions <https://github.com/AgentOxygen/GenTS/actions/workflows/conformity.yml>`_ job that produced it. Each job's log holds the commands that were run and the full per-check conformity report.

.. list-table:: Conformity results by case
    :header-rows: 1
    :widths: auto
    :class: conformity-table

    * - Case
      - Model
      - Checks passed
      - GenTS version
      - Run
    * - b.e30_alpha09d_m.B1850C_MTso_Gris.ne30_t233_wgx3.369
      - CESM3 (spec v1)
      - 92% (22/24)
      - 1.2.1.dev4+g4e9f666e2.d20260814
      - `2026-08-14 <https://github.com/AgentOxygen/GenTS/actions/runs/31837677999/attempts/1#summary-94887538773>`__

A check **passes** when it was evaluated and held, **fails** when it was evaluated and did not, and **skips** when it could not be evaluated at all (for cases that lack the relevant data). Skipped checks are excluded from the percentage rather than counted as passes, so a run over a narrow case cannot inflate its own score.

The specification version matters when comparing rows: it is bumped whenever the checks themselves change, so two percentages are only directly comparable when they were measured against the same version.

Updating the Table
------------------

Results are published deliberately rather than automatically, so that the documentation does not depend on any workflow having run. To publish a new one, dispatch the `Run Conformity Cases <https://github.com/AgentOxygen/GenTS/actions/workflows/conformity.yml>`_ workflow from the repository's Actions tab and copy the row it prints in the job summary into the table above. The workflow also runs on pull requests that touch ``gents/configs/`` or ``gents/conformity/models/``.

Running a Case Manually
-----------------------

The cases are listed in `gents/conformity/cases.json <https://github.com/AgentOxygen/GenTS/blob/main/gents/conformity/cases.json>`_.

.. code-block:: console

    docker build -f gents/conformity/Dockerfile -t gents-conformity .
    docker run --rm -v "$PWD/out:/output" gents-conformity <archive-url> <model>

The build context must be the repository root, since the image installs GenTS from source and reads ``.git`` to resolve its version. The entrypoint downloads the archive, runs ``run_gents --no-data``, then runs ``gents_conform``, writing its report to ``out/report.json``. It exits non-zero if any check failed.

To run ``gents_conform`` directly:

.. code-block:: console

    gents_conform ./my_case_output -i ./my_case_input --model CESM3

The ``-i`` argument is optional; without it, the checks that compare the output against its source are skipped rather than failed. ``gents_conform --list-models`` lists the available specifications.

See `gents/conformity/README.md <https://github.com/AgentOxygen/GenTS/blob/main/gents/conformity/README.md>`_ in the repository for how to audit a specification, change one, or add a new model.
