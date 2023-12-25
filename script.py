import os
import requests
import zipfile
import io
import json


# DOWNLOAD DATA FROM QUALTRICS

apiToken = 'ENTER HERE'
dataCenter = 'iad1' 

headers = {
    "content-type": "application/json",
    "x-api-token": apiToken,
}


surveyIds = ['ENTER HERE']      


# loop over surveys
for surveyId in surveyIds:
    # response export
    start_export_url = f'https://{dataCenter}.qualtrics.com/API/v3/surveys/{surveyId}/export-responses'
    start_export_data = json.dumps({"format": "csv", "useLabels": True})
    start_export_response = requests.post(start_export_url, headers=headers, data=start_export_data)
    progress_id = start_export_response.json()['result']['progressId']

    # export progress
    progress_status = 'inProgress'
    while progress_status == 'inProgress':
        check_progress_url = f'https://{dataCenter}.qualtrics.com/API/v3/surveys/{surveyId}/export-responses/{progress_id}'
        check_progress_response = requests.request("GET", check_progress_url, headers=headers)
        progress_status = check_progress_response.json()['result']['status']

    if progress_status == 'failed':
        raise Exception("Export failed")
    
    file_id = check_progress_response.json()['result']['fileId']

    # download the exported file
    download_url = f'https://{dataCenter}.qualtrics.com/API/v3/surveys/{surveyId}/export-responses/{file_id}/file'
    download_response = requests.get(download_url, headers=headers, stream=True)

    # unzip and extract the .csv file
    with zipfile.ZipFile(io.BytesIO(download_response.content)) as zf:
        survey_folder = "All Modules"
        if not os.path.exists(survey_folder):
            os.mkdir(survey_folder)
        zf.extractall(survey_folder)

    print(f"Download complete for survey {surveyId}")

######################################################################
# CLEAN AND APPEND DATA

import pandas as pd
import numpy as np
import glob
import os

path = 'All Modules'  

# grab all .csv files in directory
all_files = glob.glob(path + '/*.csv')

dfs = []

# define all possible columns that can appear in any survey
keep_columns = ['Finished', 'ResponseId', 'EndDate', 'Q1', 'Q2', 'Q3_1',
                'Q4_1', 'Q5', 'Q6', 'Q7', 'Q8', 'Q9', 'Q10', 'Q12', 'SC0']

# read each .csv file and append it to the list of dataframes
for filename in all_files:
    df = pd.read_csv(filename, index_col=None, header=0, skiprows=[1,2])

    # correct missing columns
    for column in keep_columns:
        if column not in df.columns:
            df[column] = np.nan

    df = df[keep_columns]
    
    # add source column
    base = os.path.basename(filename)
    df['Module'] = base

    dfs.append(df)

# concat into a single dataframe
all_quarters_df = pd.concat(dfs, axis=0, ignore_index=True)

# all_quarters_df.to_csv('sleep_all_modules.csv', index=False)

##########################################################################################
# SEND THE DATA TO GOOGLE SHEETS

import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

scopes = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

# credentials from file
# you may need to share the sheet with the service account email address
credentials = Credentials.from_service_account_file('credentials.json', scopes=scopes)

gc = gspread.authorize(credentials)
# this google sheet key is just the url of the sheet, you can find it in your browser
gs = gc.open_by_key('ENTER HERE')

# selects the first page of the sheet
worksheet = gs.get_worksheet(0)

# wipes the sheet clean
worksheet.clear()

# upload dataframe
set_with_dataframe(worksheet, all_quarters_df)

print("\nData was written to Google Sheets")


#remove .csv files 
import shutil
shutil.rmtree(survey_folder)

print("csv files were deleted from local machine")