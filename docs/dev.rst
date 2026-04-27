Developer Guide
===============

Core Design Choices
-------------------

1. Minimal dependency stack - Keeping the number of dependencies to a minimum reduces the complexity of installing GenTS to other environments and lowers the burden of refactoring in response to major updates. Each additional package required to run GenTS is an additional layer to maintain. As of writing this documentation, the `netCDF4-python <https://github.com/Unidata/netcdf4-python>`_ package is the most critical dependency with the highest exposure.
2. Narrow Scope - GenTS only generates time series. It should not implement additional computations or modifications to the netCDF files. Such additions would add complexity at the cost of modularity. In some cases, the API should be rewritten to work more effectively with other software, but its utility should not expand to take on responsibility for tasks that other software seek to accomplish (such as CMORization or regridding).
3. Test-Driven Development - All new features and bug fixes should `start` with new, failing tests. Modifications to the source code should then seek to pass those tests (in addition to the tests that already exist).

Unit Testing
------------

Tests are written using PyTest and are located in ``gents/tests/``. A Dockerfile is provided for running these tests in a containerized environment. Alternatively, tests can be run in a locally constructed environment.

Some unit tests are stand-alone, but many rely on ``gents/tests/test_cases.py`` to generate sample history files to initialize the GenTS workflow.

Docker (recommended)
------------------------

Make sure you have `Docker <https://www.docker.com/>`_ installed on your system. Then clone the GitHub repository:

.. code-block:: console

    git clone https://github.com/AgentOxygen/GenTS.git
    cd GenTS

Build the Docker container; you should only need to do this once (unless the environment inside the container needs to be updated or changed):

.. code-block:: console

    docker build -t gents .

Now run the container. If you want to make live-edits without restarting the container, bind-mount the repo directory to ``/usr/local/gents``:

.. code-block:: console

    docker run --rm -v .:/usr/local/gents -t gents

To run individual tests, specify the ``pytest`` command:

.. code-block:: console

    docker run --rm -v .:/usr/local/gents -t gents pytest gents/tests/test_workflow.py

If making contributions to documentation, you may want to locally build the webpages before committing. ``sphinx`` and ``sphinx-autobuild`` are included in Docker image, and can be run using the following command:

.. code-block:: console

    docker run --rm -v .:/usr/local/gents -p 8000:8000 -it gents sphinx-autobuild docs docs/_build/html --host 0.0.0.0

The webpages should then be accessible via `http://localhost:8000 <http://localhost:8000>`_.

Local environment
-----------------

Make sure you have a Python installed. Ideally, create a virtual environment using ``python -m venv``, `uv <https://docs.astral.sh/uv/pip/environments/>`_, or `miniconda <https://www.anaconda.com/docs/getting-started/miniconda/main>`_ before installing GenTS and its dependencies:

.. code-block:: console

    git clone https://github.com/AgentOxygen/GenTS.git
    cd GenTS
    pip install --no-cache-dir -r requirements.txt
    pip install -e .

Then execute the tests using ``pytest``:

.. code-block:: console

    pytest gents/tests/

Benchmarking
------------

Benchmarks in ``gents/benchmarks`` are written with `Airspeed Velocity (ASV) <https://asv.readthedocs.io/en/stable/index.html>`_ to track performance across new releases. You can run benchmarks the same way you run tests. Note that if you use the container, you will need to specify ``--machine [MACHINE NAME]`` after creating a machine profile since the hostname inside a Docker container differs from the host. This may not be needed for Apptainer and Singularity ports however.

To run the benchmarks for a particular tag, for example v0.9.9:

.. code-block:: console

    cd GenTS
    docker run -v .:/usr/local/gents -v ./.asv-machine.json:/root/.asv-machine.json -it gents asv run v0.9.9^! --machine name-of-machine

After running benchmarks, you can the view the results via an ASV web server:

.. code-block:: console

    docker run -v .:/usr/local/gents -v ./.asv-machine.json:/root/.asv-machine.json -p 8080:8080 -it gents   bash -c "asv publish && asv preview --port 8080"

This hosts the webpage at ``http://127.0.0.1:8080/`` by default.