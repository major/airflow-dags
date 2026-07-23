"""
Massive ETL package — namespace container only.

The package exists for import namespacing (``from massive.sql import ...``).
No code lives here. Airflow's DAG loader scans subdirectories for ``.py``
files directly; an ``__init__.py`` is not required for discovery but prevents
import ambiguities with other packages.
"""

from __future__ import annotations
