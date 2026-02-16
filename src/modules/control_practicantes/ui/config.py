# ui/etls/control_practicantes/config.py
"""
Configuración del ETL de Control de Practicantes
Pipeline: Control Practicantes → Silver → Gold con Flags
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
    id="control_practicantes",
    name="Control Practicantes",
    icon="👨‍🎓",
    description="Pipeline: Control Practicantes → Silver → Gold con Flags",
    enabled=True,
    order=2  # Después de nómina (order=1)
)