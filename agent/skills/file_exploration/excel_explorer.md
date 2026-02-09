# Excel Explorer

## Responsibility
Read and extract data from `.xls` and `.xlsx` files. It handles multi-sheet workbooks, data type detection, and formatting quirks.

## Detailed Behavior
1.  **Load Workbook**:
    -   Open the Excel file, detecting the format/engine (e.g., `openpyxl` for xlsx, `xlrd` for xls).
    -   List all available sheet names.
2.  **Sheet Extraction**:
    -   Read specific sheets or all sheets.
    -   Identify header rows automatically or via configuration.
3.  **Data Processing**:
    -   Handle cell merges (fill with value, or keep empty).
    -   Parse dates and times correctly (avoiding float representation).
    -   Handle formulas: option to read either the formula itself or the calculated value.
4.  **Cleanup**:
    -   Remove empty rows and columns.
    -   Standardize `NaN` / `None` values.
5.  **Output**:
    -   Return data as a dictionary of Pandas DataFrames or lists of dictionaries (one per sheet).

## Example Usage
```python
from agent.skills.file_exploration import ExcelExplorer

excel_tool = ExcelExplorer()
workbook = excel_tool.read("financials.xlsx")

print(workbook.sheet_names)
sheet_data = workbook.get_sheet("Q1_Report")
# Returns list of dicts: [{'Date': '2023-01-01', 'Revenue': 5000}, ...]
```
