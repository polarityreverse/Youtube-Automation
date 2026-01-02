import os
import gspread

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CREDENTIALS_PATH = os.path.join(BASE_DIR, "credentials.json")

gc = gspread.service_account(filename=CREDENTIALS_PATH)
sh = gc.open("Youtube_Ideas")

def get_worksheet(name):
    return sh.worksheet(name)