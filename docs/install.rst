Installation
============

You can install GenTS through PyPI or from source.

pip
----

Pip can be used to easily install GenTS along with all of its dependencies in any compatable Python virtual environment.

.. code-block:: console

    pip install gents['parallel']

If you only wish to have the serial version of GenTS (without Dask for parallel computing), you can omit the ``['parallel']`` optional dependency group:

.. code-block:: console

    pip install gents


Source
------

First, clone the GitHub repository.

.. code-block:: console

    git clone https://github.com/AgentOxygen/GenTS.git
    cd GenTS

Then install the package locally using ``pip``.

.. code-block:: console

    pip install -e .['parallel']

Alternatively, you can build the Docker image and run a virtual Python environment with only GenTS and its full dependencies installed:

.. code-block:: console
    docker build -t gents:latest .
    docker run --rm -it -v .:/project gents python