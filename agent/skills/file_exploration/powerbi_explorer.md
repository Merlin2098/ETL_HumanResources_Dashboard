# Power BI Explorer

## Responsibility
Inspect Power BI (`.pbix`, `.pbit`) files to extract data models, relationships, and metadata.

## Detailed Behavior
1.  **Package Inspection**:
    -   Treat the file as a ZIP archive (which it is internally).
    -   Extract key components: `DataModel`, `Report/Layout`.
2.  **Data Model Extraction**:
    -   Parse the internal data model schema.
    -   Extract table names, column names, and data types.
    -   Identify relationships between tables.
3.  **Measure Extraction**:
    -   Extract DAX measure definitions and calculated columns.
4.  **Export**:
    -   (Partial Support) If data is embedded, attempt to export underlying datasets to JSON or CSV.
    -   Generate a report of the data model structure.

## Example Usage
```python
from agent.skills.file_exploration import PowerBIExplorer

pbi_tool = PowerBIExplorer()
model = pbi_tool.analyze("dashboard.pbix")

print(model.tables)
print(model.measures) # List of DAX formulas
```
