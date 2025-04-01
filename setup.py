from setuptools import setup, find_packages
import os

# Read the content of README.md
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="looker-validator",
    version="0.1.0",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "looker-sdk==24.16.2",
        "click>=8.0.0",
        "pyyaml>=6.0",
        "tqdm>=4.65.0",
        "requests>=2.28.0",
        "colorama>=0.4.6",
    ],
    entry_points={
        "console_scripts": [
            "looker-validator=looker_validator.cli:main",
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
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Quality Assurance",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: MIT License",
    ],
)