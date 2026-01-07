# ui/etls/nomina/config.py
"""
Configuración del ETL de Nómina
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
    id="nomina",
    name="Nómina",
    icon="📊",
    description="Consolidación de planillas Metso",
    enabled=True,
    order=1
)