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
import matplotlib.pyplot as plt
from gspread.exceptions import APIError
import time
import logging

logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO,
                    format= "%(levelname)s - %(message)s - %(asctime)s")
 


class GsheetAIAuto:

    logging.info('...starting the pipeline')

    def create_client(self, credential:str) -> Client:
        logging.info('creating GSheet client')
        return gsp.authorize(credential)

    def get_spreadsheet(self, client: Client, spreadsheet_id:str):
        logging.info('...getting spreeadsheet with id %s', spreadsheet_id)
        return client.open_by_key(spreadsheet_id)

    def read_dataset(self, file_path:str, no_of_rows: int):
        logging.info('..importing dataset')
        df = pd.read_csv(file_path).head(no_of_rows).drop('Unnamed: 0', axis=1).fillna("") 
        return [df.values.tolist() , df.columns.tolist()]


    # def read_dataset1(self,file_path:str, no_of_rows: int):
    #     return pd.read_csv(file_path).head(no_of_rows).drop('Unnamed: 0', axis=1).fillna("") 


    def clean_sheet(self, spreadsheet:Spreadsheet):
        logging.info('Removing unnessary worksheet in the spreadsheet')
        ss_worksheet = [ws.title for ws in spreadsheet.worksheets()] 
        for ss_ws in ss_worksheet:
            if ss_ws not in ['raw_data', 'staging','processed']:
                spreadsheet.del_worksheet(spreadsheet.worksheet(ss_ws))
                logging.info('Deleted %s', ss_ws)

    def get_worksheet(self, spreadsheet:Spreadsheet, worsheet_title:str, row:int, col:int ):
        logging.info('Getting worksheet %s',worsheet_title )
        ss_worksheet = [ws.title for ws in spreadsheet.worksheets()] # map(lambda x:x.title, spreadsheet.worksheets())  #[ws.title for ws in spreadsheet.worksheets()] 
        #print(ss_worksheet) 
        if worsheet_title in ss_worksheet:
            logging.info(f"{worsheet_title} worksheet found")
            return spreadsheet.worksheet(worsheet_title)
        else:
            logging.info(f"{worsheet_title} does not exists, need to create one")
            return spreadsheet.add_worksheet(title=worsheet_title, rows=row , cols=col )
        
        


    # def  upload_rows_to_gsheets(self, worksheet: Worksheet,
    #                             record: List,
    #                             col_names: List):
    #     if isinstance(worksheet, Worksheet):
    #         if worksheet:      
    #             worksheet.clear() 
    #         # worksheet.update('A1', [col_names, *record])
    #             worksheet.update([col_names, *record], 'A1')


    #     print('Record uploaded successfully!!!')
    
    def upload_rows_to_gsheets(self, worksheet: Worksheet, records: List, 
                               col_names: List[str], protected: bool = False):
        """
        Upload data to Google Sheets safely.
        
        Arguments:
            worksheet: gspread Worksheet instance
            records: List of lists (rows) or list of dicts
            col_names: List of column names
            protected: If True, worksheet is read-only and won't be cleared
        """
        
        logging.info(f"Uploading data to %s worksheet", worksheet  )
        if not isinstance(worksheet, Worksheet):
            raise ValueError("Invalid worksheet provided.")
        
        # Convert list-of-lists to list-of-dicts if needed
        if len(records) > 0 and isinstance(records[0], list):
            records = [dict(zip(col_names, row)) for row in records]

        if 'id' not in [col.lower() for col in col_names]:
            # Add id to each row for idempotency
            for i, row in enumerate(records):
                row['id'] = i + 1

        try:
            # Fetch existing records for idempotency
            existing_records = worksheet.get_all_records()
            existing_ids = {(r.get('id') or r.get('Id')) for r in existing_records if 'id'in r or  "Id" in r}

            #print(existing_records)
            #print(existing_ids)
            logging.info(f"{len(existing_ids)} number of records already exists")

            # Only keep new rows that don’t exist yet
            new_records = [row for row in records if (row.get('id') or row.get('Id')) not in existing_ids]

            logging.info(f"Adding {len(new_records)} new records")

            if not new_records:
                logging.info(f"No new records to update in worksheet %s ",worksheet.title)
                return {"status": "success", "message": "No new records to update."}

            # If not protected, clear the worksheet
            if not protected:
                worksheet.clear()

            #get the keys and value that will be updated to the sheet
            keys = list(records[0].keys())
            values = [keys] + [list(row.values()) for row in records]  # all rows including existing ones

            worksheet.update('A1', values)
            logging.info(f"Records uploaded successfully to '{worksheet.title}'!")

            return {"status": "success", "message": f"{len(new_records)} new records uploaded."}

        except APIError as e:
            if "[403]" in str(e):
                raise Exception(f"Permission denied for sheet '{worksheet.title}'. Check credentials.")
            else:
                raise Exception(f"APIError while updating sheet '{worksheet.title}': {e}")

        except Exception as e:
            raise Exception(f"Error while uploading to sheet '{worksheet.title}': {e}")


    def process_stg_data(self, df:pd.DataFrame) -> Dict:
        logging.info('Processing the staging data')
        staging_df = df.copy()
        #print('staging dataframe')
        #print(staging_df.head())

        cols_list = staging_df.columns.tolist()
        logging.info("Standardizing the data column names")
        refined_cols_list = [col.replace(' ','_').lower().strip() for col in cols_list]
    
        staging_df.columns = refined_cols_list
        #standardize string columns
        logging.info("Normalizing the review text")
        str_cols = staging_df.select_dtypes(include=['object']).columns
        for col in str_cols:
            staging_df[col] = staging_df[col].str.strip().str.lower()
            
        processed_df = staging_df

        return {"processed_cols":processed_df.columns.tolist(),
                
                "processed_records":processed_df.values.tolist()
        }
    

    def pull_gsheet_data_to_df(self, worksheet_name: Worksheet, process:True) -> Dict:
        logging.info(f"Pulling data from {worksheet_name} worksheet")
        if isinstance(worksheet_name, Worksheet):
            
            sheet_data = worksheet_name.get_all_records() #worksheet_name.get_all_values() 
            if process == True:
                process_data = self.process_stg_data(pd.DataFrame(sheet_data))
        
                #print(process_data)
            
                return {"sheet_data": process_data.get("processed_records"),
                        "worksheet": worksheet_name,
                        "sheet_cols": process_data.get("processed_cols")}
            else: 
                return pd.DataFrame(sheet_data)


        
    def apply_groqAI(
        self,
        api_key: str,
        df: pd.DataFrame,
        model: str,
        review_column: str,
        sentiment_column: str = "AI Sentiment",
        summary_column: str = "AI Summary",
        batch_size: int = 10
    ):
        """
        Summarizes staging reviews using Groq,
        processed in batches to reduce API calls.
        Output format is a list of dicts.
        Ensures no empty summaries.
        """
        logging.info('Using Groq AI')
        client = Groq(api_key=api_key)

        df_col = [col.replace("_", " ").title() for col in df.columns]
        df.columns = df_col

        df[summary_column] = None
        df[sentiment_column] = None

        df_copy = df.copy()

        system_prompt = """
            You are an expert women clothing e-commerce review summarizer.

            Your tasks for EACH review:
            1. Produce a clear one-sentence summary.
            2. Assign sentiment: "positive", "negative", or "neutral".
            3. If text is too short to summarize, output summary=text

            RETURN FORMAT (STRICT):
            Return ONLY a Python list of dictionaries like:
            [
                {"summary": "...", "sentiment": "positive|negative|neutral"}
            ]
        """

        # Process in batches
        for start in range(0, df_copy.shape[0], batch_size):
            batch_df = df_copy.iloc[start:start + batch_size].copy()
            batch_df[review_column] = batch_df[review_column].fillna("").astype(str)

            # Filter valid vs empty reviews
            valid_mask = batch_df[review_column].str.strip() != ""
            valid_reviews_df = batch_df[valid_mask]
            invalid_reviews_df = batch_df[~valid_mask]

            valid_reviews_list = valid_reviews_df[review_column].tolist()

            # If all reviews empty, skip AI call
            if len(valid_reviews_list) == 0:
                for idx in invalid_reviews_df.index:
                    df_copy.at[idx, summary_column] = ""
                    df_copy.at[idx, sentiment_column] = "neutral"
                continue

            user_prompt = f"Reviews = {valid_reviews_list}"

            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.3,
                max_completion_tokens=1024,
            )

            results = response.choices[0].message.content.strip()

            # Safely convert string to list
            try:
                results = ast.literal_eval(results)
            except Exception:
                # Fallback if AI output cannot be parsed
                results = [{"summary": review, "sentiment": "neutral"} for review in valid_reviews_list]

            # Apply results to valid reviews
            for idx, result in zip(valid_reviews_df.index, results):
                summary = result.get("summary") if isinstance(result, dict) else valid_reviews_df.at[idx, review_column]
                sentiment = result.get("sentiment") if isinstance(result, dict) else "neutral"

                if not summary.strip():
                    summary = valid_reviews_df.at[idx, review_column]
                if sentiment not in ["positive", "negative", "neutral"]:
                    sentiment = "neutral"

                df_copy.at[idx, summary_column] = summary
                df_copy.at[idx, sentiment_column] = sentiment

            # Apply neutral to invalid reviews
            for idx in invalid_reviews_df.index:
                df_copy.at[idx, summary_column] = ""
                df_copy.at[idx, sentiment_column] = "neutral"

            time.sleep(5)
        logging.info('Done getting the AI Summary and AI Sentiments')
        new_df = self.set_action_needed(df_copy)
        logging.info('Added the Action Needed column!')
        return new_df



    def set_action_needed(self, df:pd.DataFrame, col_name='Action Needed'):
        logging.info("Adding the Action needed? column")
        df[col_name] = df["AI Sentiment"].apply(lambda x: "Yes" if x == "negative" else "No")

        return df


    

