import pytest
from pytest_mock import MockFixture
from src.utils import GsheetAIAuto
from configs.config import creds, sheet_id, csv_path ,GROQ_API_KEY
from gspread.client import Client
from gspread import Spreadsheet
from gspread.worksheet import Worksheet
import pandas as pd
from pathlib import Path
from typing import List


 

@pytest.fixture()
def init_object():
    return [GsheetAIAuto(), creds, sheet_id, csv_path ,GROQ_API_KEY]

def test_create_client(mocker:MockFixture,init_object):
    test_obj = init_object[0]
    mock_response = mocker.Mock(spec=Client)
    #mock_response.return_value = Client

    mock_create = mocker.patch(
        "src.utils.GsheetAIAuto.create_client",
        return_value = mock_response
    )

    test_create = test_obj.create_client(init_object[1])
    assert isinstance(test_create, mocker.Mock)



def test_get_spreadsheet(mocker: MockFixture, init_object):

    test_obj = init_object[0]
    mock_response_cl = mocker.Mock(spec=Client)
    #mock_response.return_value = Client

    mock_create = mocker.patch(
        "src.utils.GsheetAIAuto.create_client",
        return_value = mock_response_cl
    )

    test_create = test_obj.create_client(init_object[1])
 
    mock_response_ss = mocker.Mock(spec=Spreadsheet)

    mock_spreadsheet = mocker.patch(
        "src.utils.GsheetAIAuto.get_spreadsheet",
        return_value = mock_response_ss
    )

    test_spreadsheet = test_obj.get_spreadsheet(test_create, sheet_id)

    assert isinstance(test_spreadsheet, Spreadsheet)



def test_read_dataset(tmp_path, init_object):
    # create a test CSV file with "Unnamed: 0" column
    csv = tmp_path / "test_dataset.csv"
    df = pd.DataFrame({
        "Unnamed: 0": [0,1,2],
        "Review Text": ["The cloth is lovely","I don't like the cloth material","Where can I get this cloth?"],
        "Clothing ID": [10,11,12]
    })
    #save it as csv in the temporary path
    df.to_csv(csv, index=False)

    test_obj = init_object[0]
    list_values = test_obj.read_dataset(str(csv), 3)

    #print(list_values[0])
    assert isinstance(list_values[0], List)
    assert len(list_values[0]) == 3
    assert "Unnamed: 0" not in list_values[1]

 
     
def test_clean_sheet(mocker: MockFixture, init_object):
    mock_spreadsheet = mocker.Mock(spec=Spreadsheet)
    ws_raw_data = mocker.Mock(title="raw_data")
    ws_stg = mocker.Mock(title="staging")
    ws_sheet1 = mocker.Mock(title="Sheet1")
    
    mock_spreadsheet.worksheets.return_value = [ws_raw_data, ws_sheet1, ws_stg]
    
     
    mock_spreadsheet.worksheet.side_effect = lambda title: {
        "raw_data": ws_raw_data,
        "staging": ws_stg,
        "Sheet1": ws_sheet1
    }.get(title)

    test_obj = init_object[0]
    test_obj.clean_sheet(mock_spreadsheet)

   
    mock_spreadsheet.del_worksheet.assert_any_call(ws_sheet1)
    
    with pytest.raises(AssertionError):
        mock_spreadsheet.del_worksheet.assert_any_call(ws_stg)



def test_get_worksheet(mocker: MockFixture, init_object):
    mock_ss = mocker.Mock(spec=Spreadsheet)
    
    ws_raw_data = mocker.Mock(title="raw_data")
    ws_stg = mocker.Mock(title="staging")
    mock_ss.worksheets.return_value = [ws_raw_data, ws_stg]
    
    # Mock the worksheet() method to return specific sheets, when a worksheet name is passed
    mock_ss.worksheet.side_effect = lambda title: {
        "raw_data": ws_raw_data,
        "staging": ws_stg
    }.get(title)
    
    test_obj = init_object[0]
    
    # Test getting existing worksheet
    result1 = test_obj.get_worksheet(mock_ss, "raw_data", 3, 3)
    mock_ss.worksheet.assert_called_with("raw_data")
    
    #Test getting non-existing worksheet (should create it)
    ws_prc = test_obj.get_worksheet(mock_ss, "processed", 3, 3)
    mock_ss.add_worksheet.assert_called_with(title="processed", rows=3, cols=3)


# def test_upload_rows_to_gsheets(mocker: MockFixture, init_object):
#     mock_ws = mocker.Mock(spec= Worksheet)

#     test_obj = init_object[0]
#     records = [ [0, "The cloth is lovely",10],
#                [1, "I don't like the cloth material",11],
#               [ 2, "Where can I get this cloth?",12] ]
        
#     col_names = ["Unnamed: 0", "Review Text", "Clothing ID"]

#     test_obj.upload_rows_to_gsheets(mock_ws, records, col_names)

#     mock_ws.clear.assert_called_once()
#     mock_ws.update.assert_called_once()
    
def test_upload_rows_to_gsheets(mocker, init_object):
    mock_ws = mocker.Mock(spec=Worksheet)

    mock_ws.title.return_value = "TestSheet"  
    mock_ws.get_all_records.return_value = []  

    test_obj = init_object[0]

    records = [
        [0, "The cloth is lovely", 10],
        [1, "I don't like the cloth material", 11],
        [2, "Where can I get this cloth?", 12],
    ]

    col_names = ["Unnamed: 0", "Review Text", "Clothing ID"]

    test_obj.upload_rows_to_gsheets(mock_ws, records, col_names)

    mock_ws.clear.assert_called_once()
    mock_ws.update.assert_called_once()



def test_process_stg_data(init_object):
    
    test_df = pd.DataFrame({
        "Review Text": ["The cloth is lovely","I don't like the cloth material","Where can I get this cloth?"],
        "Clothing ID": [10,11,12]
    })
    

    test_obj = init_object[0]
    test_result = test_obj.process_stg_data(test_df)

    #print(list_values[0])
    
    assert test_result.get("processed_records")[0][0] == "the cloth is lovely"
    assert "Review Text" not in test_result.get("processed_cols")
    assert "review_text" in test_result.get("processed_cols")




def test_pull_gsheet_data_to_df(mocker: MockFixture, init_object):
    worksheet = mocker.Mock(spec=Worksheet)
    worksheet.get_all_records.return_value = [
        {"Review Text": "The cloth is lovely", "Clothing ID": 10},
        {"Review Text": "I don't like the cloth material", "Clothing ID": 11},
        {"Review Text": "Where can I get this cloth?",  "Clothing ID": 12}
    ]
    test_obj = init_object[0]
    test_result_true = test_obj.pull_gsheet_data_to_df(worksheet, process=True)
    assert test_result_true["sheet_data"][0][0] == "the cloth is lovely"
    
    test_result_false = test_obj.pull_gsheet_data_to_df(worksheet, process=False)
    assert isinstance(test_result_false, pd.DataFrame)
    assert test_result_false.shape[0] == 3



def test_apply_apply_groqAI(init_object):
    
    test_df = pd.DataFrame({
        "Review Text": ["The cloth is lovely","I don't like the cloth material","Where can I get this cloth?"],
        "Clothing ID": [10,11,12]
    })

    test_obj = init_object[0]
    test_result = test_obj.apply_groqAI(
        api_key=GROQ_API_KEY,
        df=test_df,
        model= "openai/gpt-oss-120b",
        review_column="Review Text",
        batch_size=2
    )
    
    assert isinstance(test_result, pd.DataFrame)
    assert test_result['AI Sentiment'].values.tolist() == ["positive", "negative", "neutral"]
   
   

def test_set_action_needed(init_object):   
    test_df = pd.DataFrame({
        "Review Text": ["The cloth is lovely","I don't like the cloth material","Where can I get this cloth?"],
        "Clothing ID": [10,11,12],
        "AI Sentiment": ["positive", "negative", "neutral"]
    })
    
    test_obj = init_object[0]
    test_result = test_obj.set_action_needed(test_df, 'Action Needed')
    assert test_result.loc[0, "Action Needed"] == "No"
    assert test_result.loc[1, "Action Needed"] == "Yes"
    assert test_result.loc[2, "Action Needed"] == "No"



