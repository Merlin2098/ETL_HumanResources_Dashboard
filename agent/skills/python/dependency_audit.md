# Skill: dependency_audit

The agent analyzes project dependencies for security vulnerabilities and licensing issues.

## Responsibility
Scan dependencies for known vulnerabilities, outdated packages, and license compliance.

## Rules
- Regularly audit dependencies for security issues
- Use pip-audit or safety for vulnerability scanning
- Keep dependencies updated to latest secure versions
- Review licenses for compliance with project requirements
- Remove unused dependencies
- Pin versions for reproducibility
- Document dependency update process

## Behavior

### Step 1: Scan for Vulnerabilities
- Run pip-audit or safety check
- Review vulnerability reports
- Prioritize critical and high-severity issues
- Update vulnerable packages

### Step 2: Check for Outdated Packages
- Use pip list --outdated
- Evaluate update risks vs benefits
- Test updates in isolated environment
- Update incrementally

### Step 3: Audit Licenses
- Use pip-licenses to list package licenses
- Identify incompatible licenses
- Document license compliance
- Avoid GPL in proprietary software (if applicable)

### Step 4: Clean Up Dependencies
- Remove unused packages
- Analyze dependency tree
- Identify duplicate or conflicting dependencies

## Example Usage

**Using pip-audit:**
```bash
# Install pip-audit
pip install pip-audit

# Scan for vulnerabilities
pip-audit

# Scan specific requirements file
pip-audit -r requirements.txt

# Output as JSON
pip-audit --format json
```

**Using safety:**
```bash
# Install safety
pip install safety

# Check for vulnerabilities
safety check

# Check specific requirements
safety check -r requirements.txt

# Generate report
safety check --json
```

**Check Outdated Packages:**
```bash
# List outdated packages
pip list --outdated

# Update specific package
pip install --upgrade package-name

# Update all (careful!)
pip list --outdated --format=freeze | grep -v '^\-e' | cut -d = -f 1 | xargs -n1 pip install -U
```

**Audit Licenses:**
```bash
# Install pip-licenses
pip install pip-licenses

# List all licenses
pip-licenses

# Output as JSON
pip-licenses --format=json

# Find specific license type
pip-licenses | grep MIT
```

**Python Script for Dependency Audit:**
```python
import subprocess
import json
from typing import List, Dict

def run_pip_audit() -> List[Dict]:
    """Run pip-audit and return vulnerabilities."""
    result = subprocess.run(
        ['pip-audit', '--format', 'json'],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        return json.loads(result.stdout)
    return []

def check_outdated_packages() -> List[Dict]:
    """Check for outdated packages."""
    result = subprocess.run(
        ['pip', 'list', '--outdated', '--format', 'json'],
        capture_output=True,
        text=True
    )
    return json.loads(result.stdout)

def audit_licenses() -> List[Dict]:
    """Get license information for all packages."""
    result = subprocess.run(
        ['pip-licenses', '--format', 'json'],
        capture_output=True,
        text=True
    )
    return json.loads(result.stdout)

def generate_audit_report():
    """Generate comprehensive dependency audit report."""
    print("=== Dependency Audit Report ===\n")
    
    # Check vulnerabilities
    vulns = run_pip_audit()
    print(f"Vulnerabilities found: {len(vulns)}")
    for vuln in vulns:
        print(f"  - {vuln.get('name')}: {vuln.get('description')}")
    
    # Check outdated
    outdated = check_outdated_packages()
    print(f"\nOutdated packages: {len(outdated)}")
    for pkg in outdated[:10]:  # Show first 10
        print(f"  - {pkg['name']}: {pkg['version']} -> {pkg['latest_version']}")
    
    # Check licenses
    licenses = audit_licenses()
    print(f"\nTotal packages: {len(licenses)}")
    
    # Group by license
    license_groups = {}
    for pkg in licenses:
        lic = pkg.get('License', 'Unknown')
        license_groups.setdefault(lic, []).append(pkg['Name'])
    
    print("\nLicense distribution:")
    for lic, pkgs in license_groups.items():
        print(f"  {lic}: {len(pkgs)} packages")

if __name__ == "__main__":
    generate_audit_report()
```

**Automated CI/CD Integration:**
```yaml
# GitHub Actions example
name: Dependency Audit

on:
  schedule:
    - cron: '0 0 * * 0'  # Weekly
  push:
    branches: [main]

jobs:
  audit:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.10'
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install pip-audit safety
      - name: Run pip-audit
        run: pip-audit
      - name: Run safety check
        run: safety check
```

## Notes
- Automate dependency audits in CI/CD pipeline
- Review vulnerability severity before updating
- Test updates thoroughly before deploying
- Subscribe to security advisories for critical packages
- Consider using Dependabot or Renovate for automated updates
