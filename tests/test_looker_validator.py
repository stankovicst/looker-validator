"""
Test suite for looker-validator package.
"""

import os
import unittest
import tempfile
from unittest.mock import patch, MagicMock, PropertyMock

from looker_validator.config import Config
from looker_validator.connection import LookerConnection
from looker_validator.validators.sql_validator import SQLValidator
from looker_validator.validators.content_validator import ContentValidator
from looker_validator.validators.assert_validator import AssertValidator
from looker_validator.validators.lookml_validator import LookMLValidator


class TestConfig(unittest.TestCase):
    """Test configuration handling."""

    def test_config_precedence(self):
        """Test that parameters override environment variables and config file."""
        # Create temp config file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml") as config_file:
            config_file.write("base_url: https://config-file.looker.com\n")
            config_file.write("client_id: config_file_id\n")
            config_file.write("client_secret: config_file_secret\n")
            config_file.flush()
            
            # Set environment variables
            with patch.dict(os.environ, {
                "LOOKER_BASE_URL": "https://env-var.looker.com",
                "LOOKER_CLIENT_ID": "env_var_id",
                "LOOKER_CLIENT_SECRET": "env_var_secret"
            }):
                # Create config with direct parameters
                config = Config(
                    config_file=config_file.name,
                    base_url="https://param.looker.com",
                    client_id="param_id"
                )
                
                # Parameter should take precedence
                self.assertEqual(config.base_url, "https://param.looker.com")
                self.assertEqual(config.client_id, "param_id")
                
                # Env var should take precedence over config file
                self.assertEqual(config.client_secret, "env_var_secret")


class TestConnection(unittest.TestCase):
    """Test Looker API connection."""

    @patch("looker_sdk.init40")
    def test_connection_init(self, mock_init40):
        """Test connection initialization."""
        # Mock SDK
        mock_sdk = MagicMock()
        mock_init40.return_value = mock_sdk
        
        # Create connection
        connection = LookerConnection(
            base_url="https://test.looker.com",
            client_id="test_id",
            client_secret="test_secret"
        )
        
        # Assert SDK was initialized
        mock_init40.assert_called_once()
        
        # Assert environment variables were set
        self.assertEqual(os.environ["LOOKERSDK_BASE_URL"], "https://test.looker.com")
        self.assertEqual(os.environ["LOOKERSDK_API_VERSION"], "4.0")
        self.assertEqual(os.environ["LOOKERSDK_CLIENT_ID"], "test_id")
        self.assertEqual(os.environ["LOOKERSDK_CLIENT_SECRET"], "test_secret")


class TestSQLValidator(unittest.TestCase):
    """Test SQL validator."""
    
    def setUp(self):
        """Set up for SQL validator tests."""
        # Mock connection
        self.mock_connection = MagicMock()
        self.mock_sdk = MagicMock()
        self.mock_connection.sdk = self.mock_sdk
        
        # Create validator
        self.validator = SQLValidator(
            connection=self.mock_connection,
            project="test_project",
            concurrency=1
        )
    
    def test_filter_explores(self):
        """Test filtering of explores based on selectors."""
        # Set up test data
        explores = [
            {"model": "model_a", "name": "explore_1"},
            {"model": "model_a", "name": "explore_2"},
            {"model": "model_b", "name": "explore_3"},
        ]
        
        # Test with include selector
        self.validator.explore_selectors = ["model_a/*"]
        filtered = self.validator._filter_explores(explores)
        self.assertEqual(len(filtered), 2)
        self.assertEqual(filtered[0]["model"], "model_a")
        self.assertEqual(filtered[1]["model"], "model_a")
        
        # Test with exclude selector
        self.validator.explore_selectors = ["-model_a/explore_1"]
        filtered = self.validator._filter_explores(explores)
        self.assertEqual(len(filtered), 2)
        self.assertEqual(filtered[0]["model"], "model_a")
        self.assertEqual(filtered[0]["name"], "explore_2")
        
        # Test with both include and exclude
        self.validator.explore_selectors = ["model_*/*", "-model_b/*"]
        filtered = self.validator._filter_explores(explores)
        self.assertEqual(len(filtered), 2)
        self.assertEqual(filtered[0]["model"], "model_a")


class TestContentValidator(unittest.TestCase):
    """Test content validator."""
    
    def setUp(self):
        """Set up for content validator tests."""
        # Mock connection
        self.mock_connection = MagicMock()
        self.mock_sdk = MagicMock()
        self.mock_connection.sdk = self.mock_sdk
        
        # Create validator
        self.validator = ContentValidator(
            connection=self.mock_connection,
            project="test_project"
        )
    
    def test_process_folders(self):
        """Test processing of folder include/exclude lists."""
        # Mock folder structure
        mock_folders = [
            MagicMock(id="1", name="Folder 1", parent_id=None),
            MagicMock(id="2", name="Folder 2", parent_id="1"),
            MagicMock(id="3", name="Folder 3", parent_id="1"),
            MagicMock(id="4", name="Personal", is_personal=True),
        ]
        self.mock_sdk.all_folders.return_value = mock_folders
        
        # Test excluding personal folders
        self.validator.exclude_personal = True
        self.validator.folders = ["1"]
        includes, excludes = self.validator._process_folders()
        
        # Should include folder 1 and its children
        self.assertIn("1", includes)
        
        # Should exclude personal folder
        self.assertIn("4", excludes)


class TestLookMLValidator(unittest.TestCase):
    """Test LookML validator."""
    
    def setUp(self):
        """Set up for LookML validator tests."""
        # Mock connection
        self.mock_connection = MagicMock()
        self.mock_sdk = MagicMock()
        self.mock_connection.sdk = self.mock_sdk
        
        # Create validator
        self.validator = LookMLValidator(
            connection=self.mock_connection,
            project="test_project",
            severity="error"
        )
    
    def test_check_severity(self):
        """Test severity checking logic."""
        # Set up test issues
        self.validator.issues = [
            {"severity": "info", "message": "Info message"},
            {"severity": "warning", "message": "Warning message"},
        ]
        
        # Test with severity=error
        self.validator.severity = "error"
        self.assertTrue(self.validator._check_severity())
        
        # Test with severity=warning
        self.validator.severity = "warning"
        self.assertFalse(self.validator._check_severity())
        
        # Test with severity=info
        self.validator.severity = "info"
        self.assertFalse(self.validator._check_severity())


class TestAssertValidator(unittest.TestCase):
    """Test Assert validator."""
    
    def setUp(self):
        """Set up for Assert validator tests."""
        # Mock connection
        self.mock_connection = MagicMock()
        self.mock_sdk = MagicMock()
        self.mock_connection.sdk = self.mock_sdk
        
        # Create validator
        self.validator = AssertValidator(
            connection=self.mock_connection,
            project="test_project"
        )
    
    def test_filter_tests(self):
        """Test filtering of LookML tests."""
        # Set up test data
        tests = [
            {"model": "model_a", "explore": "explore_1", "name": "test_1"},
            {"model": "model_a", "explore": "explore_2", "name": "test_2"},
            {"model": "model_b", "explore": "explore_3", "name": "test_3"},
            {"model": "model_c", "explore": None, "name": "test_4"},
        ]
        
        # Test with model-specific selector
        self.validator.explore_selectors = ["model_a/*"]
        filtered = self.validator._filter_tests(tests)
        self.assertEqual(len(filtered), 3)  # Should include tests 1, 2, and 4
        
        # Tests without explores should always be included
        self.assertEqual(filtered[2]["name"], "test_4")


if __name__ == "__main__":
    unittest.main()