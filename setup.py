from setuptools import setup, find_packages
import os
import re

# Read version from __init__.py
with open(os.path.join("looker_validator", "__init__.py"), "r", encoding="utf-8") as f:
    version_match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', f.read())
    version = version_match.group(1) if version_match else "0.1.0"

# Read the content of README.md
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="looker-validator",
    version=version,
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "looker-sdk==24.16.2",
        "click>=8.0.0",
        "pyyaml>=6.0",
        "tqdm>=4.65.0",
        "requests>=2.28.0",
        "colorama>=0.4.6",
        "backoff>=2.2.1",
        "aiohttp>=3.9.1",
        "aiocache>=0.12.2",
        "httpx>=0.25.2",
        "pydantic>=2.5.2",
        "rich>=13.3.0",
    ],
    extras_require={
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
        "console_scripts": [
            "looker-validator=looker_validator.async_cli:main",
        ],
    },
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