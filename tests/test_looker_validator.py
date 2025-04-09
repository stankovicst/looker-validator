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
    
    def test_process_validation_response(self):
        """Test processing of validation response."""
        # Create mock validation response
        mock_error = MagicMock(
            severity="error",
            message="Test error message",
            file_path="test_file.view.lkml",
            line_number=42,
            explore_name="test_explore",
            model_name="test_model"
        )
        mock_response = MagicMock(errors=[mock_error])
        
        # Process the response
        issues = self.validator._process_validation_response(mock_response)
        
        # Validate the result
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["severity"], "error")
        self.assertEqual(issues[0]["message"], "Test error message")
        self.assertEqual(issues[0]["file_path"], "test_file.view.lkml")
        self.assertEqual(issues[0]["line"], 42)