# ui/etls/examen_retiro/config.py
"""
Configuración del ETL de Exámenes de Retiro
"""
from dataclasses import dataclass


@dataclass
class ETLConfig:
    """Metadata del ETL"""
    id: str
    name: str
    icon: str
    description: str
    enabled: bool = True
    order: int = 0


# Configuración de este ETL
CONFIG = ETLConfig(
    id="examen_retiro",
    name="Exámenes de Retiro",
    icon="🏥",
    description="Procesamiento de Programación de Exámenes de Retiro (con enriquecimiento CC)",
    enabled=True,
    order=4
)