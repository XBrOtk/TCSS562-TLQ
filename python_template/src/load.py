import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))

import json
import logging
from Inspector import *
import time
import boto3
import csv
import pymysql


def load_write(f, dblink, dbname, tablename, username, password, inspector):
    try:
        con = pymysql.connect(host=dblink, db=dbname, user=username, passwd=password)

        cursor = con.cursor()
        # drop tabel if exists
        cursor.execute("DROP TABLE IF EXISTS " + tablename) 
        # create table
        cursor.execute("CREATE TABLE " + tablename + " (Region VARCHAR(50), Country VARCHAR(50), `Item Type` VARCHAR(50), `Sales Channel` VARCHAR(50), `Order Priority` VARCHAR(50),`Order Date` VARCHAR(50), `Order ID` INT PRIMARY KEY AUTO_INCREMENT, `Ship Date` VARCHAR(50), `Units Sold` DOUBLE, `Unit Price` DOUBLE, `Unit Cost` DOUBLE, `Total Revenue` DOUBLE, `Total Cost` DOUBLE, `Total Profit` DOUBLE, `Gross Margin` DOUBLE, `Order Processing Time` DOUBLE ) ")
        # insert data
        sql = ("INSERT INTO "+ tablename + " (Region, Country, `Item Type` , `Sales Channel` , `Order Priority` ,`Order Date`,  `Order ID`, `Ship Date`, `Units Sold`, `Unit Price`, `Unit Cost`, `Total Revenue`, `Total Cost`, `Total Profit`, `Gross Margin`, `Order Processing Time`) VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)")
        next(f)
        for line in f:
            line = [None if cell == '' else cell for cell in line]
            cursor.execute(sql, line)
            
        con.commit()
        cursor.close()
    except pymysql.MySQLError as e:
        inspector.addAttribute("error", "ERROR: Unexpected error: Could not connect to MySQL instance.")
        inspector.addAttribute("stackTrace", e)

def handler(request, context):
    # Import the module and collect data
    inspector = Inspector()

    key = str(request['key'])
    bucketname = str(request['bucketname'])
    dblink = str(request['dblink'])
    dbname = str(request['dbname'])
    tablename = str(request['tablename'])
    username = str(request['username'])
    password = str(request['password'])

    #read csv from s3
    s3 = boto3.client('s3')
    csv_file = s3.get_object(Bucket=bucketname, Key=key)
    f = csv.reader(csv_file["Body"].read().decode('utf-8').split('\n')[:-1])
    load_write(f, dblink, dbname, tablename, username, password, inspector)

    return inspector.finish()


if __name__ == "__main__":
    context = None
    request = {}
    request["key"] = "Processed_100_Sales_Records.csv"
    request["bucketname"] = "test.bucket.562f20.leb"
    request["dblink"] = "local"
    request["dbname"] = "tlq562"
    request["tablename"] = "562_test"
    request["username"] = "root"
    request["password"] = "password"

    response = handler(request, context)
    print(response)

