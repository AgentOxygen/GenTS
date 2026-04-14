Installation
============

You can install GenTS through PyPI, Container, or from source.

pip
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

Then install the package locally using ``pip``.

.. code-block:: console

    pip install -e .

Alternatively, you can build the Docker image and run a virtual Python environment with only GenTS and its full dependencies installed:

.. code-block:: console

    docker build -t gents:latest .
    docker run --rm -it -v .:/usr/local/gents gents python
