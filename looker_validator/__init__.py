"""
Looker Validator - A continuous integration tool for Looker and LookML.

Looker Validator runs validators which perform tests on your Looker instance
and your LookML. Each validator interacts with the Looker API to run tests
that ensure your Looker instance is running smoothly.

Validators:
- SQL Validator: Tests the `sql` field of each dimension for database errors
- Content Validator: Tests for errors in Looks and Dashboards
- Assert Validator: Runs Looker data tests
- LookML Validator: Runs Looker's LookML Validator to test for syntax errors
"""

__version__ = "0.1.0"