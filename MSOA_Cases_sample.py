
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

#Get latest cases data (format changed end Oct 2020)
url="https://coronavirus.data.gov.uk/downloads/msoa_data/MSOAs_latest.csv"
c=pd.read_csv(url)

#Remove lines with zero cases to reduce size (breaks Google sheets)
c = c[c.newCasesBySpecimenDateRollingSum.notnull()]

#Import MSOAs to get region and local authority, data via https://github.com/drkane/geo-lookups
msoa_url="https://raw.githubusercontent.com/drkane/geo-lookups/master/msoa_la.csv"
msoas=pd.read_csv(msoa_url)

#Join to MSOA data and remove unneeded columns
casesData = pd.merge(c, msoas, how='inner', left_on='areaCode', right_on='MSOA11CD')
casesData.drop(['areaCode','areaName','MSOA11NM','LAD17CD','LAD20CD','UTLACD','UTLANM','CAUTHCD','CAUTHNM','RGNCD','CTRYCD','CTRYNM','EWCD','EWNM','GBCD','GBNM','UKCD','UKNM'], axis=1, inplace=True)

#Iterate throgh dataframe (Credit: https://www.danielecook.com/from-pandas-to-google-sheets/)
def iter_pd(df):
    for val in df.columns:
        yield val
    for row in df.to_numpy():
        for val in row:
            if pd.isna(val):
                yield ""
            else:
                yield val

#Send Pands to sheets (Credit: https://www.danielecook.com/from-pandas-to-google-sheets/)
def pandas_to_sheets(pandas_df, sheet, clear = True):
    # Updates all values in a workbook to match a pandas dataframe
    if clear:
        sheet.clear()
    (row, col) = pandas_df.shape
    cells = sheet.range("A1:{}".format(gspread.utils.rowcol_to_a1(row + 1, col)))
    for cell, val in zip(cells, iter_pd(pandas_df)):
        cell.value = val
    sheet.update_cells(cells)

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

#Get credentials
credentials = ServiceAccountCredentials.from_json_keyfile_name('gsheets.json', scope)
gc = gspread.authorize(credentials)

#Workbook code:
workbook = gc.open_by_key("SPREADSHEET_URL_CODE")
#Sheet Name:
sheet = workbook.worksheet("MSOA_Cases")
#Upload
pandas_to_sheets(casesData, workbook.worksheet("MSOA_Cases"))
