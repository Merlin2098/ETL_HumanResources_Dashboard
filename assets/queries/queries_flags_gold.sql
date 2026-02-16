-- ============================================================================
-- QUERIES PARA GENERACIÓN DE FLAGS EN CAPA GOLD
-- Versión: 2.0
-- Fecha: 2025-12-30
-- Descripción: Calcula flags de edad y contrato usando CTEs en DuckDB
-- Autor: Richi - Data Engineering Pipeline
-- ============================================================================
-- 
-- NOTAS TÉCNICAS:
-- - Todas las fechas se calculan dinámicamente usando CURRENT_DATE
-- - Los años de nacimiento objetivo se calculan restando la edad del año actual
-- - El tiempo de servicio se calcula en años decimales (días / 365.25)
-- - Las flags son booleanas (TRUE/FALSE)
-- 
-- DEPENDENCIAS:
-- - Tabla registrada en DuckDB: "empleados"
-- - Columnas requeridas: "NUMERO DE DOC", "FECHA DE NAC.", "FECH_INGR.", 
--                        "Fecha de Termino", "Modalidad de Contrato"
-- ============================================================================

-- CTE 1: Cálculos base
-- Extrae componentes temporales y calcula métricas intermedias
WITH base_calculos AS (
    SELECT *,
           YEAR("FECHA DE NAC.") as año_nacimiento,
           YEAR(CURRENT_DATE) as año_actual,
           DATE_DIFF('day', "FECH_INGR.", COALESCE("Fecha de Termino", CURRENT_DATE)) / 365.25 as años_servicio,
           -- Calcular tiempo de servicio formateado directamente
           CONCAT(
               CAST(FLOOR(DATE_DIFF('day', "FECH_INGR.", COALESCE("Fecha de Termino", CURRENT_DATE)) / 365.25) AS INTEGER), ' años, ',
               CAST(FLOOR((DATE_DIFF('day', "FECH_INGR.", COALESCE("Fecha de Termino", CURRENT_DATE)) / 365.25 - 
                          FLOOR(DATE_DIFF('day', "FECH_INGR.", COALESCE("Fecha de Termino", CURRENT_DATE)) / 365.25)) * 12) AS INTEGER), ' meses'
           ) as tiempo_servicio_texto
    FROM empleados
),

-- CTE 2: Flags de edad (65 y 70 años)
-- Evalúa si el empleado cumple la edad objetivo en el año actual o próximo
flags_edad AS (
    SELECT 
        "NUMERO DE DOC",
        -- Edad 65 años
        (año_nacimiento = año_actual - 65) as cumple_65_esteaño,
        (año_nacimiento = (año_actual + 1) - 65) as cumple_65_proximoaño,
        -- Edad 70 años
        (año_nacimiento = año_actual - 70) as cumple_70_esteaño,
        (año_nacimiento = (año_actual + 1) - 70) as cumple_70_proximoaño
    FROM base_calculos
),

-- CTE 3: Flags de contrato
-- Evalúa si el empleado está próximo a cumplir el tiempo máximo de contrato
flags_contrato AS (
    SELECT 
        "NUMERO DE DOC",
        -- Contrato por obra o servicio específico (alerta desde 4.5 años)
        ("Modalidad de Contrato" = 'CONTRATO POR OBRA O SERVICIO ESPECIFICO' 
         AND años_servicio >= 4.5) as alerta_contrato_obra,
        -- Contrato por incremento de actividad (alerta desde 2.5 años)
        ("Modalidad de Contrato" = 'CONTRATO POR INCREMENTO DE ACTIVIDAD' 
         AND años_servicio >= 2.5) as alerta_contrato_incremento
    FROM base_calculos
)

-- SELECT FINAL: Join de registros originales + tiempo de servicio + todas las flags
-- Retorna el dataset completo con 1 columna de texto + 6 columnas booleanas
SELECT 
    e.*,
    bc.tiempo_servicio_texto,
    fe.cumple_65_esteaño,
    fe.cumple_65_proximoaño,
    fe.cumple_70_esteaño,
    fe.cumple_70_proximoaño,
    fc.alerta_contrato_obra,
    fc.alerta_contrato_incremento
FROM empleados e
LEFT JOIN base_calculos bc ON e."NUMERO DE DOC" = bc."NUMERO DE DOC"
LEFT JOIN flags_edad fe ON e."NUMERO DE DOC" = fe."NUMERO DE DOC"
LEFT JOIN flags_contrato fc ON e."NUMERO DE DOC" = fc."NUMERO DE DOC";