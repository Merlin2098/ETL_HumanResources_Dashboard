# Skill: streamlit_lifecycle_state

The agent manages the Streamlit execution lifecycle by effectively using session state and callbacks to maintain application consistency and avoid redundant reruns.

## Responsibility
Handle persistent application state and event-driven updates within the Streamlit execution model.

## Rules
- Use `st.session_state` to persist data across script reruns
- Implement callbacks (`on_change`, `on_click`) for input widgets to handle logic before the rest of the script executes
- Avoid frequent use of `st.rerun()`; prefer state-driven UI updates
- Initialize state variables at the beginning of the script or within a dedicated initialization function
- Use keys for widgets that need to maintain state through complex interactions

## Behavior

### Step 1: State Initialization
- Check if critical state variables exist in `st.session_state`
- Initialize missing variables with default values to prevent `AttributeError`

### Step 2: Callback Implementation
- Define clear, single-responsibility functions for widget events
- Pass arguments to callbacks using `args` or `kwargs` parameters in widgets
- Update `st.session_state` within callbacks to trigger UI changes

### Step 3: Lifecycle Management
- Manage the order of execution to ensure state is updated before it's displayed
- Use `st.fragment` (if available) for localized reruns of specific UI sections

## Example Usage

**State Initialization and Callbacks:**
```python
import streamlit as st

# Step 1: Initialization
if 'counter' not in st.session_state:
    st.session_state.counter = 0

# Step 2: Define Callback
def increment_counter(step: int):
    st.session_state.counter += step

# Step 3: Usage in UI
st.write(f"Current count: {st.session_state.counter}")
st.button("Increment", on_click=increment_counter, args=(1,))
st.button("Double", on_click=lambda: increment_counter(st.session_state.counter))
```

**Managing Form State:**
```python
with st.form("my_form"):
    name = st.text_input("Name")
    submitted = st.form_submit_button("Submit")
    if submitted:
        st.session_state.last_submission = name
        st.success(f"Submitted: {name}")
```

## Notes
- `st.session_state` is unique per user session
- Callbacks execute BEFORE the main script body during a rerun
- Be cautious with mutable objects in session state; changes might not trigger reruns unless the reference changes or a rerun is forced
