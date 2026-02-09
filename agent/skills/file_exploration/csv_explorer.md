# CSV Explorer

## Responsibility
Read CSV files robustly, handling various delimiters, encodings, and common data quality issues.

## Detailed Behavior
1.  **Sniffing and Detection**:
    -   Detect file encoding (UTF-8, Latin-1, etc.).
    -   Sniff the dialect: delimiter (comma, tab, semicolon), quote character, and line terminator.
2.  **Reading**:
    -   Read the file into a structured format (e.g., Pandas DataFrame or list of dicts).
    -   Handle missing headers (generate default names `col1`, `col2`...) or extra headers.
3.  **Validation**:
    -   Check for consistent column counts across rows.
    -   Identify potential data type mismatches in columns.
    -   Detect duplicate rows.
4.  **Cleaning**:
    -   Strip whitespace from string values.
    -   Handle various representations of null (e.g., "NA", "null", "-", "").
5.  **Output**:
    -   Return the cleaned dataset and a report on any rows skipped or fixed.

## Example Usage
```python
from agent.skills.file_exploration import CSVExplorer

csv_tool = CSVExplorer()
dataset = csv_tool.read("raw_data.csv", fix_encoding=True)

print(dataset.columns)
print(dataset.head())
```
