-- queries/query_licencias_agregadas.sql
-- 
-- Query para agregar motivos de licencias (CON_GOCE y SIN_GOCE)
-- y unirlos con la nómina consolidada Gold
--
-- Input: 
--   - nomina: Parquet Gold de nóminas
--   - licencias: Parquet Silver de licencias consolidadas
--
-- Output: Nómina enriquecida con columnas MOTIVO_CON_GOCE y MOTIVO_SIN_GOCE
--
-- Lógica de negocio:
--   - Si un empleado tiene múltiples licencias del mismo tipo en el mismo periodo,
--     los motivos se concatenan con " | "
--   - LEFT JOIN desde nómina: preserva todos los empleados aunque no tengan licencias
--   - NULL en MOTIVO_* indica que el empleado no tuvo licencias de ese tipo

WITH licencias_con_goce AS (
    SELECT 
        "DNI/CEX",
        PERIODO,
        STRING_AGG(MOTIVO, ' | ' ORDER BY MOTIVO) AS MOTIVO_CON_GOCE
    FROM licencias
    WHERE TIPO_LICENCIA = 'CON_GOCE'
    GROUP BY "DNI/CEX", PERIODO
),
licencias_sin_goce AS (
    SELECT 
        "DNI/CEX",
        PERIODO,
        STRING_AGG(MOTIVO, ' | ' ORDER BY MOTIVO) AS MOTIVO_SIN_GOCE
    FROM licencias
    WHERE TIPO_LICENCIA = 'SIN_GOCE'
    GROUP BY "DNI/CEX", PERIODO
)
SELECT 
    n.*,
    cg.MOTIVO_CON_GOCE,
    sg.MOTIVO_SIN_GOCE
FROM nomina n
LEFT JOIN licencias_con_goce cg 
    ON n."DNI/CEX" = cg."DNI/CEX" 
    AND n.PERIODO = cg.PERIODO
LEFT JOIN licencias_sin_goce sg 
    ON n."DNI/CEX" = sg."DNI/CEX" 
    AND n.PERIODO = sg.PERIODO