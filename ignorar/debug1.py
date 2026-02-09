# diagnostic.py
import os
import time
from pathlib import Path

base = r"C:\Users\ricuculm\OneDrive - Metso\Documents\Laboratorio\2. Reportabilidad\REPORTABILIDAD\Analisis de nómina\prueba"
expected = Path(base) / "gold" / "nomina" / "actual" / "Planilla_Metso_Consolidado.parquet"

print(f"Ruta esperada por pipeline: {expected}")
print(f"¿Existe?: {expected.exists()}")

if expected.exists():
    print(f"Tamaño: {expected.stat().st_size:,} bytes")
    print(f"Modificado: {time.ctime(expected.stat().st_mtime)}")
else:
    print("\nBuscando archivos similares...")
    parquet_files = list(Path(base).rglob("*Planilla*Consolidado*.parquet"))
    for f in parquet_files:
        rel = f.relative_to(base)
        print(f"• {rel} ({f.stat().st_size:,} bytes)")