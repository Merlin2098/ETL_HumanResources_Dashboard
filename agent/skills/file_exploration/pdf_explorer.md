# PDF Explorer

## Responsibility
Read PDF files to extract text, metadata, tables, and images. It handles complex layouts and multi-column documents, optionally leveraging OCR for scanned content.

## Detailed Behavior
1.  **Load and Validate**:
    -   Open the PDF file securely.
    -   Check if the PDF is encrypted/password-protected; handles decryption if credentials are provided.
2.  **Metadata Extraction**:
    -   Extract document metadata: Author, Creator, CreationDate, Title, Subject.
3.  **Text Extraction**:
    -   Extract text content page by page.
    -   Preserve layout information where possible (e.g., identifying headers, footers).
    -   Handle multi-column layouts by grouping text blocks logically.
4.  **Table Extraction**:
    -   Identify table structures.
    -   Extract data into structured formats (e.g., list of lists, JSON).
5.  **Image/OCR Handling**:
    -   Extract embedded images.
    -   (Optional) If text extraction yields low confidence or empty results, trigger OCR (using `pytesseract` or similar) on page images.
6.  **Output Formatting**:
    -   Return a structured object containing metadata, full text, extracted tables, and paths to extracted images.

## Example Usage
```python
from agent.skills.file_exploration import PDFExplorer

pdf_tool = PDFExplorer()
content = pdf_tool.extract("contract_signed.pdf", perform_ocr=True)

print(content['metadata']['Author'])
print(content['text']) # Full text
for table in content['tables']:
    print(table)
```
