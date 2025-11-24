import ast
import os
from dotenv import load_dotenv, dotenv_values

from google.oauth2.service_account import Credentials


load_dotenv()



#SCOPES = os.getenv('SCOPES')

#SCOPES = ast.literal_eval(SCOPES) 

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.readonly"]
sheet_id = os.getenv('sheet_id')

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CRED_FILE = os.path.join(BASE_DIR, r"configs\credentials.json")

creds =  Credentials.from_service_account_file(CRED_FILE, scopes=SCOPES)

csv_path = os.getenv('csv_path')
GROQ_API_KEY = os.getenv('GROQ_API_KEY')
#print(creds)
#print(creds.valid)


#print(SCOPES)sss


