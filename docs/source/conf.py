# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys

# Make the package importable for autodoc (works from a checkout without an install).
sys.path.insert(0, os.path.abspath('../../src'))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'splime'
copyright = '2026, Yastrebov Kirill'
author = 'Yastrebov Kirill'
release = '0.2.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.apidoc',
]

templates_path = ['_templates']
exclude_patterns = []

language = 'en'

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
apidoc_modules = [
    {
        'path': '../../src/spl',
        'destination': 'api',
        # 0.2.0: the implementation lives in private modules (spl._client,
        # spl.core._common); spl.client / spl.core.common are deprecated
        # warning shims scheduled for removal in 0.3.0.  Document the real
        # modules and skip the shims so builds stay warning-free.
        'include_private': True,
        'exclude_patterns': [
            # Literal paths are resolved relative to this conf.py directory;
            # the fnmatch variants are a safety net for absolute-path matching.
            '../../src/spl/client.py',
            '../../src/spl/core/common.py',
            '**/spl/client.py',
            '**/spl/core/common.py',
        ],
    },
]
