.. GenTS documentation master file, created by
   sphinx-quickstart on Thu Apr 24 12:54:03 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

GenTS documentation
===================

Generate Time Series (GenTS) is an open-source Python library and tool for converting output from earth system models and global climate models in the "history file" format to the "time series" format. GenTS utilizes a simplified Python interface to make this common post-processing task as easy as possible and leverages parallelism for optimal performance.

GenTS consolidates the conversion of history files to time series files into four steps:

#. Detect and read the metadata for all history files into a ``HFCollection`` 
#. Apply filters to include/exclude certain history files and then group them by subdirectory (such as model component) and file name pattern (such as an output stream).
#. Derive a ``TSCollection`` from the ``HFCollection`` and apply configurations/filters to obtain the desired time series files
#. Generate an embarrassingly parallel workload to write the time series files

.. image:: assets/ProcessSchematic.PNG
   :width: 600

These steps can be executed via a command-line interface (CLI) or via a Python script using the API. Model specific configurations for CESM3, CESM2, and E3SM are included in the CLI.

The GenTS framework is "model agnostic" to avoid fragile, "hard-coded" implementations that tend to break between model updates. This also means that GenTS works for multiple model configurations. Python scripts that use GenTS functions are provided for each model under `gents/configs`. These scripts are then called by the CLI.

.. toctree::
    install
    user
    dev
    api
