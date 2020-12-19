import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))

import logging
from Inspector import *
import time
import pymysql


def query_from_sql(query_string, dblink, dbname, tablename, username, password, inspector):
    try:
        rows = {}
        con = pymysql.connect(host=dblink, db=dbname, user=username, password=password)
        cursor = con.cursor()

        cursor.execute(query_string)
        rows = cursor.fetchall()
        con.close()

        # inspector.addAttribute("result", rows)
    except pymysql.MySQLError as e:
        inspector.addAttribute("error", "ERROR: Unexpected error: Could not connect to MySQL instance.")
        inspector.addAttribute("stackTrace", e)

def construct_query_string(filterBy, aggregateBy, tablename):
    aggr = ""
    for _i, key in enumerate(aggregateBy):
        for _j, val in enumerate(aggregateBy[str(key)]):
            aggr += key.upper()
            aggr += "(`"
            temp = str.replace(val, "_", " ")
            aggr += str(temp)
            aggr += "`), "
    fil = ""
    for _i, key in enumerate(filterBy):
        for _j, val in enumerate(filterBy[str(key)]):
            fil += "SELECT "
            fil += aggr
            fil += "'WHERE "
            temp_key = str.replace(key, "_", " ")
            fil += temp_key
            fil += "="
            temp_val = str.replace(val, "_", " ")
            fil += temp_val
            fil += "' AS `Filtered By` FROM "
            fil += tablename
            fil += " WHERE `"
            fil += temp_key
            fil += "`='"
            fil += temp_val
            fil += "' UNION "
    k = fil.rfind(" UNION ")
    result = fil[:k]
    result += ";"
    return result

def handler(request, context):
    inspector = Inspector()
    
    dblink = str(request['dblink'])
    dbname = str(request['dbname'])
    tablename = str(request['tablename'])
    username = str(request['username'])
    password = str(request['password'])

    request["filter"] = {}
    if "Region" in request:
        request['filter']["Region"] = request["Region"]
    if "Item Type" in request:
        request['filter']["Item Type"] = request["Item Type"]
    if "Sales Channel" in request:
        request['filter']["Sales Channel"] = request["Sales Channel"]
    if "Order Priority" in request:
        request['filter']["Order Priority"] = request["Order Priority"]
    if "Country" in request:
        request['filter']["Country"] = request["Country"]

    request['aggregate'] = {}
    request['aggregate']["avg"] = ["Order Processing Time", "Gross Margin", "Units Sold"]
    request['aggregate']["max"] = ["Units Sold"]
    request['aggregate']["min"] = ["Units Sold"]
    request['aggregate']["sum"] = ["Units Sold", "Total Revenue", "Total Profit"]

    query_string = construct_query_string(request['filter'], request['aggregate'], tablename)
    query_from_sql(query_string, dblink, dbname, tablename, username, password, inspector)

    return inspector.finish()


if __name__ == "__main__":
    context = None
    request = {}
    request["dblink"] = "localhost"
    request["dbname"] = "562_2020fall"
    request["tablename"] = "562_test"
    request["username"] = "root"
    request["password"] = "password"
    request["Region"] = ["Asia"]
    # request["Item Type"] = ["Supplies"]
    # request["Sales Channel"] = ["Office"]
    # request["Order Priority"] = ["Medium"]
    # request["Country"] = ["Russia"]

    response = handler(request, context)
    print(response)