# Execution Report: buscarv_esteroides.py Review & Optimization

**Agent:** Senior (Hybrid)
**Mode:** ANALYZE_AND_IMPLEMENT
**Date:** 2026-02-09
**Status:** COMPLETED

---

## 1. Analysis Summary

### Task
Review `data\scripts\buscarv_esteroides.py`, test with parquet input files, optimize for performance and readability.

### Data Profile

| File | Rows | Columns | Unique Nombre | Extra-space names |
|------|------|---------|---------------|-------------------|
| Lista_Personas_Corregida.parquet | 1,707 | 3 (N, Letra, Nombre) | 1,706 | 16 |
| resultado_final.parquet | 1,957 | 2 (DNI, Nombre) | 1,957 | 1 |

### Critical Bug Found
- **Location:** Line 52-58
- **Error:** `ColumnNotFoundError: unable to find column "Nombre"`
- **Root cause:** `df_fuente.select(pl.col("Nombre").alias("valor_encontrado"))` renames the column before it is used as `right_on="Nombre"` join key. In Polars 1.38.1, the renamed column cannot be referenced by its old name.
- **Impact:** Script fails completely — no output produced.

### Additional Issues
1. Two sequential joins (exact + clean) where a single normalized join suffices.
2. No deduplication of source names before join — risk of row multiplication.
3. Uses regex `^\s+|\s+$` for trimming instead of modern `.str.strip_chars()`.
4. Redundant `COL_BASE_NOMBRE` / `COL_FUENTE_NOMBRE` (both are "Nombre").

---

## 2. Implementation Summary

### Changes Applied
**File modified:** `data\scripts\buscarv_esteroides.py`

| Change | Before | After |
|--------|--------|-------|
| Join strategy | 2 sequential left joins | 1 join on normalized key |
| Match classification | Post-join null checks | Equality check (exact) vs not-null (clean) |
| Source deduplication | None | `.unique()` on Nombre + `.unique(subset=["_nombre_norm"])` |
| Trim method | Regex `^\s+\|\s+$` | `.str.strip_chars()` |
| Column naming | Two separate constants | Single `COL_NOMBRE` |
| Lines of code | 111 | 94 |

### Logic Preserved
- Exact match priority over clean match.
- `valor_encontrado` returns the original source name (not normalized).
- `match_status` categories: MATCH_EXACTO, MATCH_LIMPIEZA, NO_ENCONTRADO.

---

## 3. Verification Evidence

### Execution
```
✅ Enriquecimiento completado
📦 Parquet generado: data\Lista_Personas_Corregida_enriched.parquet
```

### Output Validation
- Row count preserved: 1,707 = 1,707 ✅
- Match distribution: 1,376 exact + 15 clean + 316 no match = 1,707 ✅
- All matched rows have non-empty `valor_encontrado` ✅
- All NO_ENCONTRADO rows have empty `valor_encontrado` ✅
- MATCH_LIMPIEZA samples show double-space names correctly matched to single-space source ✅

---

## 4. Governance Compliance

- Protected files modified: **NONE** ✅
- Scope respected: Only `data\scripts\buscarv_esteroides.py` modified ✅
- Changes reversible: Yes (git tracked) ✅
- Python executed via venv: `.venv\Scripts\python.exe` ✅
- `agent_rules.md` untouched ✅

---

## 5. Skills Invoked

| Skill | Category | Phase |
|-------|----------|-------|
| `code_structuring_pythonic` | Python | Analysis |
| `python_venv_executor` | Runtime | Testing |
| `performance_profiler` | Python | Optimization |
| `verification_before_completion` | Governance | Verification |
