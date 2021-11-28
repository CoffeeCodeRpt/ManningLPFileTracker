import os
from sqlite3.dbapi2 import OperationalError
import sys
import sqlite3

# Create a data base - Done with a connection to a database
def getBaseFile():
    """Return the file name to use as the Database file name"""
    return os.path.splitext(os.path.basename(__file__))[0]

def connectDb():
    """Connect to database. Database will be created if it doesn't already exist"""
    database = getBaseFile() + ".db" 
    try:
        conn = sqlite3.connect(database)
    except BaseException as error:
        conn = None
        print(str(error))
    
    return conn

def coreCursor(conn, query, args):
    """Create a cursor to db and run a query"""
    result = False
    cursor = conn.cursor()
    try:
        cursor.execute(query, args)
        rows = cursor.fetchall()
        numrows = len(list(rows))
        if numrows > 0:
            result = True
    except sqlite3.OperationalError as err:
        print(str(err))
        if cursor != None:
            cursor.close()
    finally:
        if cursor != None:
            cursor.close()
    return result

def tableexists(table):
    """Tests to see if specified table exists."""
    result = False
    conn = connectDb()
    try:
        if conn != None:
            qry = "SELECT name from sqlite_master WHERE type='table' AND name=?"
            args = (table,)
            result = coreCursor(conn, qry, args)
            if conn != None:
                conn.close()
    except sqlite3.OperationalError as err:
        print(str(err))
        if conn != None:
            conn.close()
    return result

print(tableexists('testTable'))