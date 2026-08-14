import sys
import os
sys.path.insert(0, os.path.abspath('..'))
import gents
# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'GenTS'
copyright = '2026, Cameron Cummins'
author = 'Cameron Cummins'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.duration',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary'
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store', '.ipynb_checkpoints']


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'alabaster'
html_static_path = ['_static']
html_css_files = ['custom.css']

# Alabaster centres a fixed-width page; the text column is what is left of
# page_width once sidebar_width is taken out. The theme's default 940px leaves
# roughly 700px for content, which is narrow for tables and code blocks.
html_theme_options = {
    'page_width': '1200px',
    'sidebar_width': '240px',
}
