import pandas as pd
import numpy as np
from azureml import DataTypeIds
from azureml import Workspace
from sklearn import preprocessing
from azureml.errors import AzureMLConflictHttpError

CONTAINERNAME = 'fremontbridge'
STORAGEACCOUNTNAME = 'cdqjlsa01'
STORAGEACCOUNTKEY = 'meI5fWQ7clsbUJ+8dv0euXYQ2fYIi5FbY3I6iVtnI7p9niPsoxUczENBXnihRRo1S6iB+6Vu+wHwTkEgh5ZmQQ=='

blob = []
filename = []
frames = []

blob.append('Fremont_Bridge.txt')
blob.append('Road_Aurora_2014_03_14.txt')
blob.append('Weather_station_Seattle.txt')
filename.append('Fremont_Bridge.csv')
filename.append('Road_temp_Aurora_2014_03_14.csv')
filename.append('Weather_station_Seattle.csv')

sep = ["",";","\t"]
format = ['%m/%d/%Y %I:%M:%S %p', '%d.%m.%Y %H:%M', '%Y%m%d%H%M']
altformat = ['', '%m/%d/%Y %I:%M:%S %p', '']

def blob_txt_to_dataframe(CONTAINERNAME, STORAGEACCOUNTNAME, STORAGEACCOUNTKEY, BLOBNAME, LOCALFILENAME, sep):
    from azure.storage.blob import BlockBlobService
    import os.path

    block_blob_service = BlockBlobService(account_name=STORAGEACCOUNTNAME, account_key=STORAGEACCOUNTKEY)
    if not os.path.isfile(LOCALFILENAME):
        block_blob_service.get_blob_to_path(CONTAINERNAME, BLOBNAME, LOCALFILENAME)
    if sep:
        df = pd.read_csv(LOCALFILENAME, encoding="utf8", sep=sep)
    else:
        df = pd.read_csv(LOCALFILENAME, encoding="utf8")
    
    return df

for i in range(len(blob)):
    frames.append(blob_txt_to_dataframe(CONTAINERNAME,
                                        STORAGEACCOUNTNAME,
                                        STORAGEACCOUNTKEY,
                                        blob[i], filename[i], sep=sep[i]))

def row_to_datetime(dfrow, format):
    dfrow = pd.to_datetime(dfrow, format=format, errors="raise")
    return dfrow

for i in range(len(frames)):
    try:
        frames[i]["Date"] = row_to_datetime(frames[i]["Date"], format[i])
    except ValueError:
        for index, row in frames[i].iterrows():
            try:
                row["Date"] = row_to_datetime(row["Date"], format[i])
            except ValueError:
                row["Date"] = row_to_datetime(row["Date"], altformat[i])
        frames[i]["Date"] = pd.to_datetime(frames[i]["Date"])

weather_df = frames[2][frames[2]["Date"] >= frames[0].loc[0, "Date"]]
print(weather_df.head(5))

merged_df = frames[0].merge(frames[2], how='inner', on="Date", sort=True)

clmns = ["DIR","SPD","TEMP","SLP"]
merged_df = merged_df.replace(to_replace="***", value={"DIR":0})
for each in clmns:
    merged_df[each] = pd.to_numeric(merged_df[each], errors='coerce')

merged_df.fillna(method='ffill', inplace=True)

dic = {1: 0, 2: 0, 3: 1, 4: 1, 5: 1, 6: 2, 7: 2, 8: 2, 9: 3, 10: 3, 11: 3, 12: 0}
merged_df['weekday'] = merged_df['Date'].dt.dayofweek
merged_df['month'] = merged_df['Date'].dt.month
merged_df['hour'] = merged_df['Date'].dt.hour
merged_df['year'] = merged_df['Date'].dt.year

merged_df['season'] = pd.Series(0, index=merged_df.index)
for index, row in merged_df.iterrows():
        merged_df.loc[index, 'season'] = dic.get(row['Date'].month)

merged_df.to_csv("Final_Fremont.csv")

clmns = ["DIR", "SPD", "TEMP", "SLP", 'weekday', 'month', 'hour', 'year', 'season']
x = merged_df[clmns].values #returns a numpy array
min_max_scaler = preprocessing.MinMaxScaler()
x_scaled = min_max_scaler.fit_transform(x)
merged_df[clmns] = x_scaled

print(merged_df.head(50))

ws = Workspace(
    workspace_id='d33e4ef9d7584bd78cdb0a03eb4e4a43',
    authorization_token='3a4Vt12QmXXBWxjua+PumgzzPmVeJvpGngLaRmbbyyVdrK5gHn3JD7e69DMdX8+PCzOwHDKocjyDQfwbfXF8SQ==',
    endpoint='https://europewest.studioapi.azureml.net'
)

try:
    dataset = ws.datasets.add_from_dataframe(
        dataframe=merged_df,
        data_type_id=DataTypeIds.GenericCSV,
        name='FremontFinal',
        description='Fremont traffic and weather report data merged together and processed using Pyton and Visual Studio.'
    )
except AzureMLConflictHttpError:
    dataset = ws.datasets['FremontFinal']
    dataset.update_from_dataframe(merged_df)