# HTML Explorer

## Responsibility
Parse HTML documents to extract relevant content while stripping unnecessary elements. It supports DOM traversal and querying via CSS selectors or XPath.

## Detailed Behavior
1.  **Load and Clean**:
    -   Load HTML from file or URL content.
    -   Use a parser (e.g., BeautifulSoup) to build the DOM.
    -   Remove non-content tags: `<script>`, `<style>`, `<meta>`, `<noscript>`, and comments.
2.  **Content Extraction**:
    -   Extract the main content text, preserving structural formatting (paragraphs, headers).
    -   Extract specific elements using CSS selectors (e.g., `div.content`, `table.results`).
    -   Extract all links (`href` attributes) and media sources (`src` attributes).
3.  **Table parsing**:
    -   Convert HTML `<table>` elements into structured data (lists/JSON).
4.  **Metadata Extraction**:
    -   Extract page title, description, and OpenGraph metadata.

## Example Usage
```python
from agent.skills.file_exploration import HTMLExplorer

html_tool = HTMLExplorer()
page_data = html_tool.parse("index.html")

title = page_data.title
main_text = html_tool.extract_text("div#main-content")
links = html_tool.extract_links()
```
