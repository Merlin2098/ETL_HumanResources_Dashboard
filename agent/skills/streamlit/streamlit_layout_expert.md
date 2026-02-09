# Skill: streamlit_layout_expert

The agent designs professional, intuitive, and responsive Streamlit user interfaces by leveraging advanced layout components and structural containers.

## Responsibility
Organize UI elements logically and aesthetically to improve usability and data hierarchy in Streamlit dashboards and applications.

## Rules
- Use `st.columns` for side-by-side positioning of widgets and metrics
- Utilize `st.sidebar` for global controls, filters, and navigation
- Implement `st.tabs` to separate distinct workflows or perspectives in the same view
- Use `st.container` and `st.empty` for dynamic content management and grouping
- Use `st.expander` for optional content or secondary information to keep the UI clean

## Behavior

### Step 1: Structural Planning
- Define the main layout: Sidebar for inputs, Main area for outputs
- Group related widgets into containers or columns
- Establish a clear visual hierarchy using headings (`st.header`, `st.subheader`)

### Step 2: Adaptive Layouts
- Use proportional column widths (e.g., `st.columns([3, 1])`) for main/sidebar style layouts in the main area
- Implement tabs for multi-step processes or different data visualizations

### Step 3: Interactive Organization
- Position critical "Action" buttons in prominent locations
- Use help text and tooltips (`help="..."` param) for complex UI elements

## Example Usage

**Advanced Multi-Column Layout:**
```python
import streamlit as st

st.set_page_config(layout="wide")

# Sidebar for global filters
with st.sidebar:
    st.title("Filters")
    date_range = st.date_input("Select Range")

# Main Dashboard Layout
st.title("Business Intelligence Dashboard")

top_col1, top_col2, top_col3 = st.columns(3)
top_col1.metric("Revenue", "$45,000", "+5%")
top_col2.metric("Users", "12,400", "+12%")
top_col3.metric("Churn", "2.1%", "-0.5%")

tab1, tab2 = st.tabs(["📈 Analysis", "🗃 Raw Data"])

with tab1:
    col_left, col_right = st.columns([2, 1])
    with col_left:
        st.subheader("Regional Performance")
        # Visualization placeholder
    with col_right:
        with st.expander("Show Detailed Metrics"):
            st.write("Detailed breakdown by city...")

with tab2:
    st.subheader("Dataset View")
    # Data table placeholder
```

## Notes
- Streamlit layout components work well with "With" statements (Context Managers)
- Avoid over-nesting columns within columns as it may break on smaller screens
- Use `st.set_page_config(layout="wide")` to utilize the full screen width for complex dashboards
