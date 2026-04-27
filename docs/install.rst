Installation
============

GenTS is available for download as a Python package through the Python Package Index (PyPI) or as a container.

PyPI
----

Pip can be used to easily install GenTS along with all of its dependencies in any compatible Python virtual environment.

.. code-block:: console

    pip install gents

Container
---------

GenTS is available in a pre-built Docker image from DockerHub at `agentoxygen/gents`. Most HPC container platforms such as Singularity and Apptainer natively support pulling from DockerHub. For calling the CLI:

.. code-block:: console

    apptainer run docker://agentoxygen/gents:latest run_gents --help

Similarly, for accessing the Python environment:

.. code-block:: console

    apptainer run docker://agentoxygen/gents:latest python

Source
------

First, clone the GitHub repository.

.. code-block:: console

    git clone https://github.com/AgentOxygen/GenTS.git
    cd GenTS

Then, create or activate a Python virtual environment (using ``conda``, ``uv``, or Python 3 ``venv``) and install the package locally using ``pip``.

.. code-block:: console

    pip install -e .

Alternatively, you can build the Docker image and run a Python virtual environment with only GenTS and its full dependencies installed:

.. code-block:: console

    docker build -t gents:latest .
    docker run --rm -it -v .:/usr/local/gents gents python

You can also reference the ``Dockerfile`` in the GenTS source code repository for how to install from source, specifically using ``uv``.