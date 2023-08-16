from asammdf import MDF
import pandas as pd
import numpy as np


def findAllZero(df: pd.DataFrame,factor:str,time:str) -> pd.DataFrame:
    segments = []
    start = None
    for index,_ in df.iterrows():
        if df.loc[index,factor] == 0:
            if start is None:
                start = df.loc[index,time]
        else:
            if start is not None:
                segments.append([start,df.loc[index,time]])
                start = None
    if start is not None:
        segments.append([start,df.loc[len(df)-1,time]])
    stop = pd.DataFrame(columns=['start','end'],data = segments)
    return stop

def deleteZeroDf(df:pd.DataFrame,delete:pd.DataFrame)->pd.DataFrame:
    for index,_ in delete.iterrows():
        start = delete.loc[index,'start']
        end = delete.loc[index,'end']
        df = df[(df['timestamps'] < start) | (df['timestamps'] > end)]
    return df

def readMDF(filepath,cycleId):
    data = MDF(filepath,raise_on_multiple_occurrences=False)
    dataFiltered = data.to_dataframe(['VehSpdLgtSafe','ALgt1','BkpOfDstTrvld',
                            'HvBattPwr','HvThermPwrCns','RoadIncln','VehM','HvHeatrPwrCns2','AmbTIndcd'],time_as_date=True)  
    df = dataFiltered.rename(columns={'VehSpdLgtSafe':'speed',
                        'ALgt1':'acceleration',
                        'BkpOfDstTrvld':'total_driven_distance',
                        'HvBattPwr':'output_power',
                        'HvThermPwrCns':'thermal_system_power_consumption',
                        'RoadIncln':'inclination',
                        'VehM':'vehicle_mass',
                        'HvHeatrPwrCns2':'AC_consumption'})
    df['speed(km/h)'] = df['speed'] * 3.6
    df_second = df.resample('0.1S').mean().reset_index()
    return df_second

def cleanData(df_second, deleteTime):
    stop = findAllZero(df_second,'speed(km/h)','timestamps')
    stop['timeLength'] = [(row['end']-row['start']).total_seconds() for _,row in stop.iterrows()]
    delete = stop.loc[stop['timeLength']>=deleteTime]
    idling = stop.loc[stop['timeLength']<deleteTime]
    df_200 = deleteZeroDf(df_second,delete)
    df_200['label']='moving'
    for i,r in idling.iterrows():
        label_i = (df_200['timestamps'] >= r['start']) & (df_200['timestamps'] <= r['end'])
        df_200.loc[label_i,'label']='idling'
    label_changes = df_200['label'].ne(df_200['label'].shift())
    group_id = label_changes.cumsum()
    df_200['group_id']=group_id
    #generate cycle_id by combining a moving piece and idling piece
    df_200['cycle_id']=df_200['group_id']//2
    return df_200