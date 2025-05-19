#!/usr/bin/env python
"""
Bump the version number in __init__.py.
Usage: python bump_version.py [major|minor|patch]
"""

import re
import sys

# Define the file path
init_file = "looker_validator/__init__.py"

# Read the current version
with open(init_file, "r") as f:
    content = f.read()

# Extract current version
version_match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', content)
if not version_match:
    print("Error: Could not find version string in __init__.py")
    sys.exit(1)

current_version = version_match.group(1)
print(f"Current version: {current_version}")

# Parse the current version into components
try:
    major, minor, patch = map(int, current_version.split('.'))
except ValueError:
    print("Error: Version string is not in the format 'major.minor.patch'")
    sys.exit(1)

# Determine the new version based on the argument
bump_type = sys.argv[1] if len(sys.argv) > 1 else "patch"

if bump_type == "major":
    new_version = f"{major + 1}.0.0"
elif bump_type == "minor":
    new_version = f"{major}.{minor + 1}.0"
elif bump_type == "patch":
    new_version = f"{major}.{minor}.{patch + 1}"
else:
    print("Error: Bump type must be 'major', 'minor', or 'patch'")
    sys.exit(1)

print(f"New version: {new_version}")

# Replace the version string in the file
new_content = re.sub(
    r'__version__\s*=\s*["\']([^"\']+)["\']',
    f'__version__ = "{new_version}"',
    content
)

# Write the updated content back to the file
with open(init_file, "w") as f:
    f.write(new_content)

print(f"Version updated to {new_version} in {init_file}")