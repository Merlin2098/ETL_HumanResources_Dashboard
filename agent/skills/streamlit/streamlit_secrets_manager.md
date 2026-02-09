# Skill: streamlit_secrets_manager

The agent ensures secure configuration and credential management in Streamlit applications by utilizing the native secrets handling system.

## Responsibility
Protect sensitive information such as API keys, database credentials, and internal configuration from exposure in source code.

## Rules
- Use `.streamlit/secrets.toml` for local development
- Access secrets exclusively via `st.secrets` object
- Never commit `.streamlit/secrets.toml` to version control (ensure it is in `.gitignore`)
- Support multi-layer configuration (e.g., using `.env` as fallback or for non-Streamlit environments)
- Implement clear error messages or validation for missing required secrets

## Behavior

### Step 1: Secure Configuration Setup
- Identify sensitive keys required by the application
- Guide the setup of `.streamlit/secrets.toml` structure
- Add `.streamlit/secrets.toml` to `.gitignore` if not already present

### Step 2: Access & Validation
- Load credentials using the hierarchical access in `st.secrets`
- Validate that all required keys are present during application startup
- Provide fallback paths or "Developer Mode" flags if secrets are missing locally

### Step 3: Deployment Consistency
- Ensure secret keys in the cloud platform (e.g., Streamlit Community Cloud) match the local naming conventions

## Example Usage

**Accessing Secrets:**
```python
import streamlit as st

# Step 2: Safe Access
try:
    db_user = st.secrets["database"]["user"]
    db_pass = st.secrets["database"]["password"]
    api_key = st.secrets["openai_api_key"]
except (KeyError, FileNotFoundError):
    st.error("Missing configuration. Please check your secrets.toml file.")
    st.stop()

# Use credentials...
st.info(f"Connected as: {db_user}")
```

**Recommended secrets.toml Structure:**
```toml
# .streamlit/secrets.toml
openai_api_key = "sk-..."

[database]
user = "admin"
password = "password123"
host = "localhost"
```

## Notes
- `st.secrets` behaves like a dictionary and supports nested tables from TOML
- When deploying to Streamlit Community Cloud, secrets are managed in the app settings UI
- Avoid printing or displaying secrets in the UI (e.g., in `st.write`) even for debugging
