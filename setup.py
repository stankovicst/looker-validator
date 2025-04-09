#!/usr/bin/env python
# -*- coding: utf-8 -*-

# This setup.py is based *directly* on the user-provided code,
# with only 'GitPython' and 'rich' added based on analysis of other project files.

from setuptools import setup, find_packages
import os
import re

# Read version from __init__.py
# Using the exact mechanism provided by the user
try:
    with open(os.path.join("looker_validator", "__init__.py"), "r", encoding="utf-8") as f:
        version_match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', f.read())
        if version_match:
            version = version_match.group(1)
        else:
            raise RuntimeError("Unable to find version string in looker_validator/__init__.py.")
except FileNotFoundError:
     raise RuntimeError("looker_validator/__init__.py not found.")


# Read the content of README.md
# Using the exact mechanism provided by the user
try:
    with open("README.md", "r", encoding="utf-8") as fh:
        long_description = fh.read()
except FileNotFoundError:
    print("Warning: README.md not found. Using short description only.")
    long_description = "A continuous integration tool for Looker and LookML validation" # Fallback

setup(
    # All metadata below is taken directly from the user-provided code
    name="looker-validator",
    version=version,
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "looker-sdk==24.16.2", # Original dependency
        "click>=8.0.0",        # Original dependency
        "pyyaml>=6.0",         # Original dependency
        "tqdm>=4.65.0",        # Original dependency
        "requests>=2.28.0",    # Original dependency
        "colorama>=0.4.6",     # Original dependency
        # --- Dependencies added based on analysis of other project files ---
        "GitPython>=3.1.0",    # Required by branch_manager.py
        "rich>=10.0.0",        # Required by printer.py (assumed based on analysis)
        # -----------------------------------------------------------------
    ],
    extras_require={
        # Kept exactly as provided by the user
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
            "build>=0.10.0",
            "twine>=4.0.0",
        ],
    },
    entry_points={
        # Kept exactly as provided by the user
        "console_scripts": [
            "looker-validator=looker_validator.cli:main",
        ],
    },
    # All metadata below kept exactly as provided by the user
    python_requires=">=3.8",
    author="Stevan Stankovic",
    author_email="stankovicst@mediamarktsaturn.com",
    description="A continuous integration tool for Looker and LookML validation",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords="looker, lookml, validation, ci, continuous integration, testing",
    url="https://github.com/stankovicst/looker-validator",
    project_urls={
        "Bug Tracker": "https://github.com/stankovicst/looker-validator/issues",
        "Documentation": "https://github.com/stankovicst/looker-validator#readme",
        "Source Code": "https://github.com/stankovicst/looker-validator",
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Quality Assurance",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
