import os
import sys
import hashlib
import sqlite3

def basefile():
    """ Return name of base file. """
    return os.path.splitext(os.path.basename(__file__))[0]

def connectDb():
    """ Connect to core database. Database will be created if it doens't already exist. """
    database = basefile() + ".db"

    try:
        conn = sqlite3.connect(database, timeout=2)
    except BaseException as err:
        print(str(err))
        conn = None

    return conn

def coreCursor(conn, query, args):
    """ Run query against connected database. """
    result = False
    cursor = conn.cursor()
    try:
        cursor.execute(query, args)
        rows = cursor.fetchall()
        numRows = len(list(rows))

        if numRows > 0:
            result = True
    except sqlite3.OperationalError as err:
        print(str(err))
        result = False
        if cursor != None:
            cursor.close()
    finally:
        if cursor != None:
            cursor.close()
        
    return result

def tableExists(table):
    """ Runs a query to see if a table exists """
    result = False
    conn = connectDb()
    try:
        if conn != None:
            qry = "SELECT name from sqlite_master WHERE type='table' AND name=?"
            args = (table,)
            result = coreCursor(conn, qry, args)
            if conn != None:
                conn.close
    except sqlite3.OperationalError as err:
        print(str(err))
        if conn != None:
            conn.close
    return result

def createHashTable():
    """ Creates a DB Table to store hashed files. """
    result = True
    qry = "CREATE TABLE files (file TEXT md5 TEXT)"
    conn = connectDb()
    try:
        if conn != None:
            if not tableExists('files'):
                cursor = conn.cursor()
                try:
                    cursor.execute(qry)
                except sqlite3.OperationalError as err:
                    print(str(err))
                    if cursor != None:
                        cursor.close()
                finally:
                    conn.commit()
                    if cursor != None:
                        cursor.close()
                    result = True
    except sqlite3.OperationalError as err:
        print(str(err))
        if conn != None:
            conn.close()
    finally:
        if conn != None:
            conn.close()
    return result

def createHashtableIdx():
    """ Creates a SQLite DB Table Index. """
    result = False
    qry = "CREATE INDEX idxfile ON files(file, md5)"
    try:
        conn = connectDb()
        if conn != None:
            if tableExists('files'):
                try:
                    cursor = conn.cursor()
                    cursor.execute(qry)
                    result = True
                except sqlite3.OperationalError as err:
                    print(str(err))
                    if cursor != None:
                        cursor.close
    except sqlite3.OperationalError as err:
        print(str(err))
        if conn != None:
            conn.close()
    finally:
        if conn != None:
            conn.close()
    
    return result

def runcmd(qry, args):
    """ Runs specific command on SQLite Database. """
    try:
        conn = connectDb()
        if conn != None:
            if (tableExists()):
                try:
                    cursor = conn.cursor()
                    cursor.execute(qry, args)
                except sqlite3.OperationalError as err:
                    print(str(err))
                    if cursor != None:
                        cursor.close()
    except sqlite3.OperationalError as err:
        print(str(err))
        if conn != None:
            conn.close()
    finally:
        if conn != None:
            conn.close()

def updateHashtable(fname, md5):
    """ Update the SQLite file table. """
    qry = "UPDATE files SET md5=? WHERE file=?"
    args = (md5,fname)
    runcmd(qry, args)

def insertHashtable(fname, md5):
    """ Insert a file and its hash into the Hash Table. """
    args = (fname,md5)
    qry = "INSERT INTO files (file, md5) VALUES(?, ?)"
    runcmd(qry, args)

def setupHashtable(fname, md5):
    """ Set's up the Hash Table. """
    createHashTable()
    createHashtableIdx()
    insertHashtable(fname, md5)

def md5indb(fname):
    """ Checks to see if md5 hash tag exists in database"""
    items = []
    qry = "SELECT md5 WHERE file=? FROM files"
    try:
        conn = connectDb()
        if conn != None:
            if tableExists('files'):
                try:
                    cursor = conn.cursor()
                    cursor.execute(qry,fname)
                    for row in cursor():
                        items.append(row[0])
                except sqlite3.OperationalError as err:
                    print(str(err))
                    if cursor != None:
                        cursor.close
                finally:
                    if cursor != None:
                        cursor.close()
    except sqlite3.OperationalError as err:
        print(str(err))
        if conn != None:
            conn.close()
    finally:
        if conn != None:
            conn.close()
    return items

def hasChanged(fname, md5):
    """ Checks to see if a files md5 has changed"""
    result = False
    oldMd5 = md5indb(fname)
    if len(oldMd5) > 0:
        if md5 != oldMd5:
            result = True
            updateHashtable(fname, md5)
    else:
        setupHashtable(fname, md5)
    return result

def getFileExt(fname):
    """ Returns a file name extension. """
    return os.path.splitext(fname)[1]

def getModDate(fname):
    """ Returns the specified files last modified date. """
    try:
        mtime = os.path.getmtime(fname)
    except OSError as emsg:
        print(str(emsg))
        mtime = 0
    return mtime

def md5Short(fname):
    """ Get md5 file hash tag. """
    return hashlib.md5(str(fname + '|' + str(getModDate(fname))).encode('utf8')).hexdigest()

def loadFlds():
    """ Parse the configuration file and return folders to be included
     and extensions to be excluded. """
    flds = []
    ext = []
    config = basefile() + '.ini'
    if os.path.isfile(config):
        cfile = open(config, 'r')
        # Parse each config file line and get the folder and extensions.
        for line in cfile:
            if '|' in line:
                flds.append(line.split('|')[0])
                exts = line.split('|')[1]
                if len(exts) > 0:
                    ext.append(exts.split(','))
            else:
                flds.append(line)
                ext.append([])
        cfile.close()
    return flds, ext

def checkFileChanges(folder, exclude):
    """ Checks folder structure for files that have changed """
    changed = False
    for subdir, dirs, files, in os.walk(folder):
        for fname in files:
            origin = os.path.join(subdir, fname)
            if os.path.isfile(origin):
                fext = getFileExt(origin)
                if not fext in exclude:
                    md5 = md5Short(origin)
                    if hasChanged(origin, md5):
                        changed = True
                        print(f"{origin} has changed.")
    return changed

def runFileChanges():
    """ Invoke the function that loads and parses the config file """
    changed = False
    fldExts = loadFlds()
    for i , fld in enumerate(fldExts[0]):
        exts = fldExts[1]
        changed = checkFileChanges(fld, exts[i], ws)       
    return changed

