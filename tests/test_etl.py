
import pytest
from src.etl import main



def test_etl_main_exists():
    """Simple test to verify main function exists and can be imported"""
    assert callable(main)

def test_etl_main_can_be_called(mocker):

    """Test that main() can be called without crashing"""
    # Mock all the external dependencies to prevent actual execution
    mocker.patch('src.etl.GsheetAIAuto')
    mocker.patch('src.etl.ReviewAnalysis')
    mocker.patch('src.etl.creds')
    mocker.patch('src.etl.sheet_id')
    mocker.patch('src.etl.csv_path')
    mocker.patch('src.etl.GROQ_API_KEY')
    
    
    main()























