import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import ast
import pandas as pd
from typing import  Dict, List
import gspread as gsp
from configs.config import creds, sheet_id, csv_path ,GROQ_API_KEY
from gspread.client import Client
from gspread.worksheet import Worksheet
from gspread import Spreadsheet
from groq import Groq


from src.utils import GsheetAIAuto 
from src.analysis import ReviewAnalysis
 


def main():
    #instantiate the object of the classes
    gsheetauto = GsheetAIAuto()
    revana = ReviewAnalysis()
    
    #create a client that the gspread will use to access the google sheet
    client = gsheetauto.create_client(creds)
    #print()
    #access the spreadsheet
    spreadsheet = gsheetauto.get_spreadsheet(client, sheet_id)

    #read dataset as pandas dataframe
    df = gsheetauto.read_dataset(csv_path,200)
    #df = read_dataset1(csv_path,10)

    #get the shape of the datafrane
    row, col = len(df[0]) , len(df[1])
    #get worksheet, create if it doesn't exist
    raw_worksheet = gsheetauto.get_worksheet(spreadsheet , 'raw_data',row, col)
 
    #clean worksheet, so that I don't have unessary worksheet in the spreadsheet
    gsheetauto.clean_sheet(spreadsheet)
    #upload data to the worksheet
    gsheetauto.upload_rows_to_gsheets(raw_worksheet, df[0], df[1], True)
    #get staging worksheet, and create one if it doesn't exist
    stg_worksheet = gsheetauto.get_worksheet(spreadsheet , 'staging',row, col)
    #pull data from the raw_data and process it
    stg_data = gsheetauto.pull_gsheet_data_to_df(raw_worksheet, process=True)
    #print(stg_data)
    #upload the processed data to the staging sheet
    gsheetauto.upload_rows_to_gsheets(stg_worksheet, stg_data.get('sheet_data'),  stg_data.get('sheet_cols'))
    #print(stg_data.get("sheet_data"))
    #get the processed sheet and create one if it doens't exist

    prc_worksheet = gsheetauto.get_worksheet(spreadsheet , 'processed',row, col)
    #pull data from the stagin worksheet
    prc_data_check =  gsheetauto.pull_gsheet_data_to_df(prc_worksheet,process=False)

    print('>>',len(stg_data.get("sheet_data")))

    print('>>',prc_data_check.shape[0])
    
    if len(stg_data.get("sheet_data")) != prc_data_check.shape[0]:

        prc_data =  gsheetauto.pull_gsheet_data_to_df(stg_worksheet,process=False)
        #apply the groqAI to summarise test and oerfirm sentiment analysis.
        prc_data_df = gsheetauto.apply_groqAI(GROQ_API_KEY,   prc_data, "openai/gpt-oss-120b",  "Review Text",  "AI Sentiment",  "AI Summary", 10 )
        #print(prc_data.head())
        #upload Ai data into the processed sheet
        gsheetauto.upload_rows_to_gsheets(prc_worksheet, prc_data_df.values.tolist(),  prc_data_df.columns.tolist())
        #analyse the
        revana.analyze_sentiment_by_class( prc_data_df, "Class Name",  "AI Sentiment" )

    revana.analyze_sentiment_by_class( prc_data_check, "Class Name",  "AI Sentiment" )
   
if __name__ == '__main__':
    main()




 