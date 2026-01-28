-- Query: Generación de capa Gold para Control de Practicantes
-- Descripción: Calcula tiempo de servicio y flags de evaluación para practicantes profesionales
-- Input: control_practicantes_silver (tabla registrada en DuckDB)
-- Output: DataFrame con columnas enriquecidas y flags
-- 
-- CAMBIOS:
-- - Usa Fecha_Renovacion - Fecha_Ingreso en lugar de CURRENT_DATE - Fecha_Ingreso
-- - Si Fecha_Renovacion es NULL, tiempo_servicio y flags quedan NULL
-- - Renombrado: "FECHA ING" → "Fecha_Ingreso", "F. RENOVACION" → "Fecha_Renovacion"

WITH calculo_tiempo AS (
    SELECT 
        DNI,
        CONDICION,
        "FECHA ING" AS Fecha_Ingreso,
        "F. RENOVACION" AS Fecha_Renovacion,
        SEDE,
        UNIVERSIDAD,
        "JEFE INMEDIATO",
        GERENCIA,
        
        -- Calcular días de servicio: Fecha_Renovacion - Fecha_Ingreso
        -- Si Fecha_Renovacion es NULL, dias_servicio es NULL
        CASE 
            WHEN "F. RENOVACION" IS NOT NULL 
            THEN CAST("F. RENOVACION" - "FECHA ING" AS INTEGER)
            ELSE NULL
        END AS dias_servicio,
        
        -- Calcular años de servicio (división entera)
        CASE 
            WHEN "F. RENOVACION" IS NOT NULL 
            THEN CAST(FLOOR(("F. RENOVACION" - "FECHA ING") / 365.25) AS INTEGER)
            ELSE NULL
        END AS anios_servicio,
        
        -- Calcular meses de servicio (residuo convertido a meses)
        CASE 
            WHEN "F. RENOVACION" IS NOT NULL 
            THEN CAST(FLOOR((("F. RENOVACION" - "FECHA ING") % 365.25) / 30.44) AS INTEGER)
            ELSE NULL
        END AS meses_servicio
        
    FROM control_practicantes_silver
),

flags_evaluacion AS (
    SELECT 
        *,
        
        -- Flag 1: Por cumplir 1 año (falta <= 1 mes, es decir entre 335-364 días)
        -- Si dias_servicio es NULL, flag es NULL
        CASE 
            WHEN dias_servicio IS NULL THEN NULL
            WHEN CONDICION = 'PRACTICANTE PROFESIONAL' 
                AND dias_servicio BETWEEN 335 AND 364 
            THEN 'SI' 
            ELSE 'NO' 
        END AS por_cumplir_1,
        
        -- Flag 2: Ya cumplió 1 año (>= 365 días)
        CASE 
            WHEN dias_servicio IS NULL THEN NULL
            WHEN CONDICION = 'PRACTICANTE PROFESIONAL' 
                AND dias_servicio >= 365 
            THEN 'SI' 
            ELSE 'NO' 
        END AS cumplio_1,
        
        -- Flag 3: Por cumplir 2 años (falta <= 3 meses, es decir entre 640-729 días / 21-24 meses)
        CASE 
            WHEN dias_servicio IS NULL THEN NULL
            WHEN CONDICION = 'PRACTICANTE PROFESIONAL' 
                AND dias_servicio BETWEEN 640 AND 729 
            THEN 'SI' 
            ELSE 'NO' 
        END AS por_cumplir_2
        
    FROM calculo_tiempo
)

SELECT 
    DNI,
    CONDICION,
    Fecha_Ingreso,
    Fecha_Renovacion,
    SEDE,
    UNIVERSIDAD,
    "JEFE INMEDIATO",
    GERENCIA,
    
    -- Tiempo de servicio como texto formateado
    -- Si anios_servicio o meses_servicio es NULL, tiempo_servicio es NULL
    CASE 
        WHEN anios_servicio IS NULL THEN NULL
        WHEN anios_servicio = 0 THEN meses_servicio || ' meses'
        WHEN anios_servicio = 1 AND meses_servicio = 0 THEN '1 año'
        WHEN anios_servicio = 1 AND meses_servicio = 1 THEN '1 año y 1 mes'
        WHEN anios_servicio = 1 THEN '1 año y ' || meses_servicio || ' meses'
        WHEN meses_servicio = 0 THEN anios_servicio || ' años'
        WHEN meses_servicio = 1 THEN anios_servicio || ' años y 1 mes'
        ELSE anios_servicio || ' años y ' || meses_servicio || ' meses'
    END AS tiempo_servicio,
    
    dias_servicio,
    por_cumplir_1,
    cumplio_1,
    por_cumplir_2
FROM flags_evaluacion
ORDER BY Fecha_Ingreso DESC