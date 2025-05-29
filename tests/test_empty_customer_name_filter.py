import unittest
import pandas as pd
import os
import sys
from pathlib import Path
import tempfile
import shutil

# Add the project root to sys.path
sys.path.append(str(Path(__file__).resolve().parents[1]))

# Import the functions to test
from core.workflows import filter_empty_rows, run_script

class TestEmptyCustomerNameFilter(unittest.TestCase):
    """Test that only rows with empty CUSTOMER_NAME are passed to the next step."""
    
    def setUp(self):
        """Set up test environment with mock data."""
        # Create a temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()
        self.test_file_path = Path(self.temp_dir) / "test_data.csv"
        
        # Create test data with a mix of empty and non-empty CUSTOMER_NAME values
        self.test_data = pd.DataFrame({
            'Transaction Date': ['2025-05-01', '2025-05-02', '2025-05-03', '2025-05-04', '2025-05-05'],
            'DESCRIPTION': ['Transaction 1', 'Transaction 2', 'Transaction 3', 'Transaction 4', 'Transaction 5'],
            'Credit': [100.00, 200.00, 300.00, 400.00, 500.00],
            'CUSTOMER_NAME': ['Customer A', '', None, 'Customer D', '']
        })
        
        # Save test data to CSV
        self.test_data.to_csv(self.test_file_path, index=False)
        
        # Create a mock parser that will be called after filtering
        self.parser_called = False
        self.parser_input_file = None
        
    def tearDown(self):
        """Clean up temporary files."""
        shutil.rmtree(self.temp_dir)
    
    def mock_parser(self):
        """Mock parser function that records it was called and checks the input file."""
        self.parser_called = True
        
        # Read the file that would be passed to the parser
        if self.parser_input_file and Path(self.parser_input_file).exists():
            df = pd.read_csv(self.parser_input_file)
            self.filtered_data = df
            return True
        return False
    
    def test_filter_empty_customer_name(self):
        """Test that filter_empty_rows correctly filters rows with empty CUSTOMER_NAME."""
        # Call the filter_empty_rows function
        result = filter_empty_rows(self.test_file_path)
        
        # Verify the function returned True (indicating rows were filtered)
        self.assertTrue(result, "filter_empty_rows should return True when rows are filtered")
        
        # Read the filtered file
        filtered_df = pd.read_csv(self.test_file_path)
        
        # Verify only rows with empty CUSTOMER_NAME were kept
        self.assertEqual(len(filtered_df), 2, "Should have kept only 2 rows with empty CUSTOMER_NAME")
        
        # Verify all remaining rows have empty CUSTOMER_NAME
        for _, row in filtered_df.iterrows():
            customer_name = row['CUSTOMER_NAME']
            self.assertTrue(pd.isna(customer_name) or customer_name == '', 
                           f"Row should have empty CUSTOMER_NAME, but found: {customer_name}")
    
    def test_filtered_rows_passed_to_parser(self):
        """Test that only filtered rows are passed to the next step (parser)."""
        # Set up the mock parser
        self.parser_input_file = self.test_file_path
        
        # First filter the rows
        filter_empty_rows(self.test_file_path)
        
        # Then run the parser
        run_script(self.mock_parser)
        
        # Verify the parser was called
        self.assertTrue(self.parser_called, "Parser should have been called after filtering")
        
        # Verify the parser received only rows with empty CUSTOMER_NAME
        self.assertEqual(len(self.filtered_data), 2, "Parser should have received only 2 rows")
        
        # Verify all rows passed to the parser have empty CUSTOMER_NAME
        for _, row in self.filtered_data.iterrows():
            customer_name = row['CUSTOMER_NAME']
            self.assertTrue(pd.isna(customer_name) or customer_name == '', 
                           f"Parser received row with non-empty CUSTOMER_NAME: {customer_name}")

    def test_workflow_integration(self):
        """Test the integration between filtering and parsing in the workflow."""
        # Create a more complex test case with multiple files
        test_files = {
            "MBB": {
                "path": Path(self.temp_dir) / "MBB_test.csv",
                "data": pd.DataFrame({
                    'Transaction Date': ['2025-05-01', '2025-05-02'],
                    'DESCRIPTION': ['MBB Transaction 1', 'MBB Transaction 2'],
                    'Credit': [100.00, 200.00],
                    'CUSTOMER_NAME': ['', 'Customer B']  # One empty, one filled
                })
            },
            "PBB": {
                "path": Path(self.temp_dir) / "PBB_test.csv",
                "data": pd.DataFrame({
                    'Transaction Date': ['2025-05-03', '2025-05-04'],
                    'DESCRIPTION': ['PBB Transaction 1', 'PBB Transaction 2'],
                    'Credit': [300.00, 400.00],
                    'CUSTOMER_NAME': ['Customer C', '']  # One filled, one empty
                })
            },
            "SG": {
                "path": Path(self.temp_dir) / "SG_test.csv",
                "data": pd.DataFrame({
                    'Transaction Date': ['2025-05-05', '2025-05-06'],
                    'DESCRIPTION': ['SG Transaction 1', 'SG Transaction 2'],
                    'Credit': [500.00, 600.00],
                    'CUSTOMER_NAME': ['Customer E', 'Customer F']  # All filled
                })
            }
        }
        
        # Save all test files
        for bank, info in test_files.items():
            info["data"].to_csv(info["path"], index=False)
        
        # Track which parsers were called
        parsers_called = []
        
        # Create mock parsers for each bank
        def create_mock_parser(bank):
            def mock_parser():
                parsers_called.append(bank)
                return True
            return mock_parser
        
        # Test the workflow logic
        for bank, info in test_files.items():
            # Filter the file
            has_empty_rows = filter_empty_rows(info["path"])
            
            # If it has empty rows, run the parser
            if has_empty_rows:
                run_script(create_mock_parser(bank))
        
        # Verify only MBB and PBB parsers were called (they had empty CUSTOMER_NAME)
        self.assertEqual(len(parsers_called), 2, "Only 2 parsers should have been called")
        self.assertIn("MBB", parsers_called, "MBB parser should have been called")
        self.assertIn("PBB", parsers_called, "PBB parser should have been called")
        self.assertNotIn("SG", parsers_called, "SG parser should not have been called")

if __name__ == '__main__':
    unittest.main()