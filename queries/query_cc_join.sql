/*
================================================================================
QUERY: ENRIQUECIMIENTO DE EXAMENES DE RETIRO CON CENTROS DE COSTO
================================================================================
Descripción: 
  Combina información de examenes_retiro_gold con catálogos de centros de costo
  (CC_ACTUAL y CC_OLD) para enriquecer los datos con información organizacional.

Reglas de Negocio:
  1. UNION de CC_ACTUAL y CC_OLD con prioridad a CC_ACTUAL (más reciente)
  2. LEFT JOIN para mantener TODOS los registros de examenes
  3. Clasificación de registros por status de match
  4. Preservación de todas las columnas originales

Resultados Esperados:
  - 485 registros totales (100% de examenes)
  - ~183 registros con match exitoso (37.7%)
  - ~300 registros sin código CC (61.9%)
  - ~2 registros con código no encontrado (0.4%)

Autor: Richi - HR Data Analytics
Fecha: 05/01/2026
================================================================================
*/

-- ============================================================================
-- PASO 1: CREAR CATÁLOGO UNIFICADO DE CENTROS DE COSTO
-- ============================================================================
-- Combina CC_ACTUAL y CC_OLD, priorizando CC_ACTUAL cuando hay duplicados
-- mediante el uso de FIRST_VALUE con ORDER BY fuente DESC

WITH cc_unificado AS (
    SELECT DISTINCT
        CC,
        -- FIRST_VALUE toma el primer valor después de ordenar por fuente (2=ACTUAL, 1=OLD)
        FIRST_VALUE("NOMBRE CC") OVER (
            PARTITION BY CC 
            ORDER BY fuente DESC
        ) AS nombre_cc,
        FIRST_VALUE("CATEGORIA CC") OVER (
            PARTITION BY CC 
            ORDER BY fuente DESC
        ) AS categoria_cc,
        FIRST_VALUE(GERENCIA) OVER (
            PARTITION BY CC 
            ORDER BY fuente DESC
        ) AS gerencia
    FROM (
        -- Subquery 1: CC_ACTUAL con prioridad 2
        SELECT 
            CC,
            "NOMBRE CC",
            "CATEGORIA CC",
            GERENCIA,
            2 AS fuente  -- Prioridad alta
        FROM cc_actual
        
        UNION ALL
        
        -- Subquery 2: CC_OLD con prioridad 1
        SELECT 
            CC,
            "NOMBRE CC",
            "CATEGORIA CC",
            GERENCIA,
            1 AS fuente  -- Prioridad baja
        FROM cc_old
    ) combined
)

-- ============================================================================
-- PASO 2: LEFT JOIN CON EXAMENES DE RETIRO
-- ============================================================================
-- Mantiene TODOS los registros de examenes y enriquece con info de CC

SELECT 
    -- Columnas originales de examenes_retiro_gold
    e.NOMBRE,
    e.DNI,
    e."FECHA DE CESE",
    e."CAUSA DE SALIDA",
    e.CARGO,
    e."NOMBRE DE CC" AS codigo_cc_original,  -- Renombrado para claridad
    e.AÑO,
    e.MES,
    e.NOMBRE_MES,
    
    -- Columnas enriquecidas desde catálogo de CC
    cc.CC AS codigo_cc,                      -- Código CC normalizado
    cc.nombre_cc AS nombre_cc_completo,      -- Nombre descriptivo del CC
    cc.categoria_cc,                         -- Categoría (COGS, SGA, etc.)
    cc.gerencia,                             -- Gerencia responsable
    
    -- Columna de control de calidad
    CASE 
        WHEN e."NOMBRE DE CC" = '' THEN 'SIN_CODIGO'
        WHEN cc.CC IS NULL THEN 'CODIGO_NO_ENCONTRADO'
        ELSE 'OK'
    END AS status_match

FROM examenes e
LEFT JOIN cc_unificado cc 
    ON e."NOMBRE DE CC" = cc.CC  -- Join por código de centro de costo

ORDER BY e.NOMBRE;

/*
================================================================================
NOTAS TÉCNICAS
================================================================================

1. LEFT JOIN vs INNER JOIN:
   - Se usa LEFT JOIN para mantener TODOS los registros de examenes
   - Registros sin match tendrán NULL en columnas de CC

2. Manejo de duplicados en catálogo:
   - FIRST_VALUE + PARTITION BY garantiza un solo registro por CC
   - La prioridad se controla con la columna 'fuente'

3. Status de match:
   - 'OK': Match exitoso, todas las columnas de CC pobladas
   - 'SIN_CODIGO': Campo NOMBRE DE CC está vacío en origen
   - 'CODIGO_NO_ENCONTRADO': Código existe pero no está en catálogo

4. Rendimiento:
   - El CTE cc_unificado se calcula una sola vez
   - Los índices en columna CC mejorarían el JOIN (si aplica)

5. Columnas con espacios:
   - "NOMBRE CC", "CATEGORIA CC", etc. requieren comillas dobles
   - En el resultado se renombran sin espacios para facilitar uso

================================================================================
*/