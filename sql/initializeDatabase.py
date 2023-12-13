import sqlite3
import pandas as pd
import numpy as np
import glob
from pathlib import Path
import os

boolMap = {'f':0, 't':1}

# for mapping Dataframe column dtypes to SQLite types
typeDict = {np.dtype('int64'): 'INTEGER', np.dtype('object'): 'TEXT', np.dtype('float64'): 'REAL'}

listHasPrimaryKey = ['elements', 'part_categories', 'inventories', 'parts', 'minifigs', 'colors', 'sets', 'themes']

# given the dataframe read from a csv, the table name, and whether the table has a primary key, write
# the SLQ query to define the table
def makeCreateTableQuery(df, tableName, hasPrimaryKey):
    qry = f'CREATE TABLE IF NOT EXISTS {tableName} (\n'
    for n, (col, type) in enumerate(df.dtypes.items()):
        if n==0 and hasPrimaryKey:
            qry += f'  {col} {typeDict[type]} PRIMARY KEY,\n'
        elif n==len(df.columns)-1:
            qry += f'  {col} {typeDict[type]}\n'
        else:
            qry += f'  {col} {typeDict[type]},\n'
    qry += ')'
    return qry

# given the table name, create and fill the table from the corresponding csv
def MakeTableFromCSV(tableName):
    df = pd.read_csv(f'RebrickableData/{tableName}.csv')
    # map bool columns from 'f'/'t' to 0/1
    for col in df.columns:
        if list(np.sort(df[col].dropna().unique())) == ['f','t']:
            df[col] = df[col].map(boolMap)
    hasPrimaryKey = tableName in listHasPrimaryKey
    qry = makeCreateTableQuery(df, tableName, hasPrimaryKey)
    cursor.execute(qry)
    df.to_sql(tableName, conn, if_exists='append', index=False)

### main script

# create and connect to DB
conn = sqlite3.connect('sql/lego.db')
cursor = conn.cursor()

# read csv files and get table names from file names
dataFiles = glob.glob('RebrickableData/*.csv')
tableNames = [Path(filename).stem for filename in dataFiles]

# create tables
for tableName in tableNames:
    MakeTableFromCSV(tableName)

