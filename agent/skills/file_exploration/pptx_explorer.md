# PowerPoint Explorer

## Responsibility
Extract content from PowerPoint (`.pptx`) presentations, including text, speaker notes, and slide metadata.

## Detailed Behavior
1.  **Load Presentation**:
    -   Open `.pptx` file.
    -   Count total slides and identify hidden slides.
2.  **Text Extraction**:
    -   Iterate through slides.
    -   Extract text from titles, body placeholders, text boxes, and tables.
    -   Maintain the reading order of elements.
3.  **Notes Extraction**:
    -   Extract text from the speaker notes section for each slide.
4.  **Image/Media Discovery**:
    -   List images and media embedded in slides.
    -   (Optional) Save embedded images to a temporary directory.
5.  **Metadata**:
    -   Extract core properties (title, subject, author).

## Example Usage
```python
from agent.skills.file_exploration import PPTXExplorer

pptx_tool = PPTXExplorer()
presentation = pptx_tool.read("pitch_deck.pptx")

for slide in presentation.slides:
    print(f"Slide {slide.number}: {slide.title}")
    print(f"Notes: {slide.notes}")
```
