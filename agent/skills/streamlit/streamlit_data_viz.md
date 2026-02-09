# Skill: streamlit_data_viz

The agent implements highly interactive data visualizations and dynamic data editing capabilities within Streamlit applications using standard and third-party libraries.

## Responsibility
Enable users to explore, visualize, and interact with data through charts, maps, and editable data tables.

## Rules
- Prefer `st.plotly_chart` or `st.altair_chart` for complex, interactive visualizations
- Use `st.data_editor` to allow users to modify data directly in the UI
- Apply appropriate chart themes (e.g., using Streamlit's native theme integration)
- Optimize chart performance by reducing data density for overview visualizations
- Handle visualization errors gracefully with informative placeholders

## Behavior

### Step 1: Interactive Charting
- Build Plotly or Altair figures based on data requirements
- Configure tooltips, zoom, and selection interactions
- Use `use_container_width=True` to ensure charts feel responsive

### Step 2: Editable Data Planes
- Implement `st.data_editor` for CRUD-like operations on DataFrames
- Validate edited data before processing or saving
- Handle row additions and deletions if supported by the use case

### Step 3: Visualization Refinement
- Use color scales and labels that adhere to branding or accessibility standards
- Combine charts with metric widgets for a holistic overview

## Example Usage

**Interactive Plotly Visualization:**
```python
import streamlit as st
import plotly.express as px
import pandas as pd

df = pd.DataFrame({
    "City": ["SF", "NY", "LA", "CHI"],
    "Sales": [450, 700, 300, 500],
    "Profit": [120, 200, 80, 150]
})

fig = px.bar(df, x="City", y="Sales", color="Profit", title="Sales by City")
st.plotly_chart(fig, use_container_width=True)
```

**Interactive Data Editor:**
```python
import streamlit as st
import pandas as pd

if 'df' not in st.session_state:
    st.session_state.df = pd.DataFrame([
       {"name": "Apples", "stock": 10, "price": 1.2},
       {"name": "Bananas", "stock": 50, "price": 0.5},
    ])

edited_df = st.data_editor(st.session_state.df, num_rows="dynamic")

if st.button("Save Changes"):
    st.session_state.df = edited_df
    st.success("Inventory updated!")
```

## Notes
- `st.data_editor` returns a copy of the changed dataframe
- For very large datasets, use aggregation or sampling before visualizing to maintain performance
- Always consider the color-blind friendly palettes for visualizations
