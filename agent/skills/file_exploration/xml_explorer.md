# XML Explorer

## Responsibility
Parse XML content, validate structure against schemas (XSD), and extract specific nodes or attributes. It is robust against namespaces and can handle malformed XML within reason.

## Detailed Behavior
1.  **Parsing**:
    -   Load XML string or file.
    -   Parse the document tree.
    -   Handle `lxml` or `xml.etree.ElementTree` parsing errors with descriptive messages.
2.  **Validation**:
    -   (Optional) If an XSD schema is provided, validate the XML against it.
    -   Report validation errors including line numbers and specific constraints violated.
3.  **Namespace Handling**:
    -   Detect and register namespaces to simplify XPath queries.
    -   Clean or normalize namespaces if requested for easier processing.
4.  **Extraction**:
    -   Execute XPath queries to select nodes, attributes, or text content.
    -   Convert complex sub-trees into dictionary/JSON representations.
5.  **Sanitization**:
    -   Attempt to repair common XML syntax errors (e.g., unclosed tags) if "lenient mode" is enabled.

## Example Usage
```python
from agent.skills.file_exploration import XMLExplorer

xml_tool = XMLExplorer()
data = xml_tool.parse("config.xml", schema="config.xsd")

# Get specific setting
timeout = xml_tool.query("//setting[@name='timeout']/@value")

# Convert to dict
config_dict = xml_tool.to_dict()
```
