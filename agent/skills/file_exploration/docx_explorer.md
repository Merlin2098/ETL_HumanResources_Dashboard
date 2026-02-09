# Word Explorer

## Responsibility
Read Microsoft Word (`.docx`) documents to extract text, tables, and structural elements.

## Detailed Behavior
1.  **Load Document**:
    -   Open `.docx` file.
    -   Access the main document body, headers, and footers.
2.  **Text Extraction**:
    -   Iterate through paragraphs.
    -   Extract text while preserving basic styling hooks (bold, italic) or flattening to plain text.
    -   Read headers and footers.
3.  **Table Extraction**:
    -   Identify tables within the document.
    -   Extract table cell contents into structured lists/grids.
4.  **Section Handling**:
    -   Handle documents with multiple sections (different headers/orientations).
5.  **Metadata**:
    -   Extract document properties (author, revision number, created/modified dates).

## Example Usage
```python
from agent.skills.file_exploration import DocxExplorer

docx_tool = DocxExplorer()
doc = docx_tool.read("specs.docx")

print(doc.text) # Full text
for table in doc.tables:
    process_table(table)
```
