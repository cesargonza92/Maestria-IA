"""Empaquetado minimo del pipeline para el runner portable (Flink).

Cuando se somete el job via PortableRunner (`--setup_file=setup.py`), Beam
arma un sdist con estos modulos y lo instala en cada worker (contenedor
`apache/beam_python3.11_sdk`) antes de ejecutar los DoFn -- necesario porque
esos workers no comparten el sistema de archivos del contenedor `beam_pipeline`
que somete el job, a diferencia del DirectRunner (todo en un solo proceso).
No aplica para DirectRunner: ahi los modulos ya estan en el mismo filesystem.
"""

from setuptools import setup

setup(
    name="fraud_pipeline",
    version="1.0.0",
    py_modules=["validation", "dedup", "rules", "io_kafka", "pipeline"],
    install_requires=["jsonschema>=4.0"],
)
