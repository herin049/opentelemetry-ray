# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys

# Add the package source directory so autodoc can find modules.
sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "opentelemetry-resource-detector-ray",
            "src",
        ),
    ),
)
sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "opentelemetry-instrumentation-ray-serve",
            "src",
        ),
    ),
)

project = "OpenTelemetry Ray"
copyright = "Lukas Hering"  # noqa: A001
author = "Lukas Hering"


extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "opentelemetry": (
        "https://opentelemetry-python.readthedocs.io/en/latest/",
        None,
    ),
}

autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
}

nitpicky = True
nitpick_ignore = [
    (
        "py:class",
        "opentelemetry.instrumentation.instrumentor.BaseInstrumentor",
    ),
]

html_theme = "sphinx_rtd_theme"
