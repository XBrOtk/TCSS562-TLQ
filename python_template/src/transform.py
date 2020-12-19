import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))

import json
import logging
from Inspector import *
import time
import boto3
import numpy as np
import pandas as pd
import json
from io import StringIO


def transform(csv):
    df = csv
    df = df.drop_duplicates(subset=['Order ID'], keep='first')   #Remove duplicate data identified by [Order ID]

    # Transform [Order Priority] column
    col_no = [i for i in df.columns].index('Order Priority') 
    for i in range(len(df)):
        if df.iloc[i, col_no] == "L":
            df.iloc[i, col_no] = "Low"
        elif df.iloc[i, col_no] == "M":
            df.iloc[i, col_no] = "Medium"
        elif df.iloc[i, col_no] == "H":
            df.iloc[i, col_no] = "High"
        elif df.iloc[i, col_no] == "C":
            df.iloc[i, col_no] = "Critical"

    # Add a [Gross Margin] column
    Gross_Margin = df['Total Profit'].values / df['Total Revenue'].values
    df['Gross Margin'] = Gross_Margin

    # Add column [Order Processing Time]
    Ship_Date = df['Ship Date'].values.tolist()
    for i in range(len(Ship_Date)):
        Ship_Date[i] = Ship_Date[i].split('/')
        Ship_Date[i] = list(map(int, Ship_Date[i]))

    Order_Date = df['Order Date'].values.tolist()
    for i in range(len(Order_Date)):
        Order_Date[i] = Order_Date[i].split('/')
        Order_Date[i] = list(map(int, Order_Date[i]))

    Order_Processing_Time = [0 for _ in range(len(df))]
    for i in range(len(df)):
        Order_Processing_Time[i] = (Ship_Date[i][2] - Order_Date[i][2]) * 365 + (Ship_Date[i][0] - Order_Date[i][0]) * 30 + (Ship_Date[i][1] - Order_Date[i][1])

    #write CSV
    df['Order Processing Time'] = Order_Processing_Time

    return df

def handler(request, context):
    # Import the module and collect data
    inspector = Inspector()

    key = str(request['key'])
    bucketname = str(request['bucketname'])
    
    # read csv from s3
    s3 = boto3.client('s3')
    csv_file = s3.get_object(Bucket=bucketname, Key=key)
    csv = pd.read_csv(csv_file['Body'])
    # transform csv
    csv_new = transform(csv)
    csv_buffer = StringIO()
    csv_new.to_csv(csv_buffer, index=False)
    
    content = csv_buffer.getvalue()
    # write csv to s3
    s3.put_object(Bucket=bucketname, Key="Processed_{}".format(key), Body=content)

    return inspector.finish()


if __name__ == "__main__":
    context = None
    request = {}
    request["key"] = "100_Sales_Records.csv"
    request["bucketname"] = "test.bucket.562f20.leb"

    response = handler(request, context)
    print(response)
