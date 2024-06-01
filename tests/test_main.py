import unittest
from unittest.mock import patch, MagicMock
from main import main

class TestMain(unittest.TestCase):
    @patch('main.create_spark_session')
    @patch('main.parse_args')
    @patch('main.run_analysis')
    def test_main(self, mock_run_analysis, mock_parse_args, mock_create_spark_session):
        # Mock arguments
        mock_parse_args.return_value = MagicMock(n=20, input="input/data.csv", output="save", output_path="output/output.csv", undirected=True)
        
        # Mock Spark session creation
        mock_spark = MagicMock()
        mock_create_spark_session.return_value = mock_spark
        
        # Mock run_analysis to avoid actual computation
        mock_run_analysis.return_value = None
        
        main()
        
        # Check if run_analysis was called with the correct parameters
        mock_run_analysis.assert_called_once_with(mock_spark, 20, "input/data.csv", "save", "output/output.csv", True)

if __name__ == '__main__':
    unittest.main()
