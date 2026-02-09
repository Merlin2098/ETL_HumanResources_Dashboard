---
name: library_manager
description: Manages Python libraries and dependencies, ensuring they are installed, documented, and consistent across the project.
---

# Library Manager

## Responsibility
Ensure that any new feature, analysis script, or Python best practice implementation has its required libraries properly managed. This skill guarantees that dependencies are not only installed but also tracked in `requirements.txt` with clear documentation, maintaining environment consistency.

## Behavior

### 1. Dependency Check
Before running or suggesting code that uses external libraries, the skill MUST check the project's `requirements.txt` file (located at the project root) to verify if the library is already listed.

### 2. Authorization Request
If a required library is missing from `requirements.txt`:
- The skill MUST PAUSE and request explicit authorization from the user to add the new dependency.
- It should explain *why* the library is needed and what feature it supports.

### 3. Installation
Upon receiving authorization:
- Install the library using the standard package manager (e.g., `pip install <library_name>`).
- Ensure the installation is successful before proceeding.

### 4. Manifest Update
Immediately after installation, the skill MUST add the library to `requirements.txt`.
- The entry MUST include a brief comment explaining the library's purpose.
- Format: `library_name==version  # Purpose: <brief explanation>`

### 5. Consistency Enforcement
- The skill ensures that all environments (dev, test, prod) rely on the same `requirements.txt`.
- It avoids ad-hoc installations that are not recorded in the manifest.

## Usage Examples

### Example: Adding specific data analysis library
**Scenario**: A new script requires `pandas` for data manipulation.

1.  **Check**: search `requirements.txt` for `pandas`.
2.  **Result**: Not found.
3.  **Action**: Ask user: "The script requires 'pandas' for data frames. May I install it and add it to requirements.txt?"
4.  **On Approval**: 
    - Run: `pip install pandas`
    - Update `requirements.txt`:
      ```text
      pandas==2.2.0  # Purpose: Data manipulation and analysis for sales reports
      ```

### Example: Using existing library
**Scenario**: A script needs `requests` for API calls.

1.  **Check**: search `requirements.txt` for `requests`.
2.  **Result**: Found `requests==2.31.0`.
3.  **Action**: Proceed with code generation using the installed version.
