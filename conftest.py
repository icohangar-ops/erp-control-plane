"""Pytest root configuration.

Makes the repository root importable so tests can exercise the ``api``
package, which is a runtime namespace package rather than a pip-installed
distribution (pyproject packages are limited to connectors* and
control_plane*).
"""
