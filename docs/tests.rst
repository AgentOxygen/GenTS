Testing
=======

Tests are written using PyTest and are located in ``gents/tests/``. A Dockerfile is provided for running these tests in a containerized environment. Alternatively, tests can be run in a locally constructed environment.

Some unit tests are stand-alone, but many rely on ``gents/tests/test_cases.py`` to generate sample history files to initialize the GenTS workflow.

Docker (recommended)
------------------------

Make sure you have `Docker <https://www.docker.com/>`_ installed on your system. Then clone the GitHub repository:

.. code-block:: console

    git clone https://github.com/AgentOxygen/GenTS.git
    cd GenTS

Build the Docker container, you should only need to do this once (unless the environment needs to be updated or changed):

.. code-block:: console

    docker build -t gents .

Now run the container. Make sure to bind the repo directory to the ``/project`` mount:

.. code-block:: console

    docker run --rm -v .:/project -t gents:latest

To run individual tests, specify the ``pytest`` command:

.. code-block:: console

    docker run --rm -v .:/project -t gents:latest pytest gents/tests/test_workflow.py

If making contributions to documentation, you may want to locally build the webpages before committing. ``sphinx`` and ``sphinx-autobuild`` are included in Docker image, and can be run using the following command:

.. code-block:: console

    docker run --rm -v .:/project -p 8000:8000 -it gents:latest sphinx-autobuild docs docs/_build/html --host 0.0.0.0

The webpages should then be accessible via `http://localhost:8000 <http://localhost:8000>`_.

Local environment
-----------------

Make sure you have a Python instance installed. Ideally, create a virtual environment using ``python -m venv`` or `miniconda <https://www.anaconda.com/docs/getting-started/miniconda/main>`_ before installing GenTS and its dependencies:

.. code-block:: console

    git clone https://github.com/AgentOxygen/GenTS.git
    cd GenTS
    pip install --no-cache-dir -r requirements.txt
    pip install pytest
    pip install -e .

Then execute the tests using ``pytest``:

.. code-block:: console

    pytest gents/tests/