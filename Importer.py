# -*- coding: utf-8 -*-
"""
Created on Fri Oct 14 15:15:45 2016

@author: Arnold Choa
"""

import sqlite3 as sq
import csv
import os

def representsInt(s):
    """ Check if string is possibly an integer
    s is a string
    Return True if possibly to be integer; False otherwise
    """
    try: 
        int(s)
        return True
    except ValueError:
        return False
    
    
def initial(dbname):
    """Establish Connection to SQL database
    If it does not yet exist, create database
    
    dbname is the filepath of the database
    
    Return connection and cursor
    """
    
    conn = sq.connect(dbname+'.db')
    cur = conn.cursor()
    
    return conn,cur

def close(conn):
    """Close Connection to SQL database
    conn is the connection of the database
    If Y, save changes. If N, do not save changes. If C, cancel closing.
    Otherwise, loop.
    """
    
    choice = 0
    
    while not choice in ['Y','N','C','y','n','c']:
        
        choice = input("Do you want to save changes? Y/N. "\
                       "C to cancel closing.\t")
        
        if choice=='C' or choice=='c':
            return
        elif choice=='N' or choice=='n':
            conn.close()
            return
        elif choice=='Y' or choice=='y':
            conn.commit()
            conn.close()
            return

def createTable(cur, name, colname, coltype):
    """Creates a table inside a database. first column must be primary key
    cur  - cursor
    name - table name
    nf   - column name
    ft   - column 1 field type
    """
    
    try:
        cur.execute('CREATE TABLE {tn} ({nf} {ft} PRIMARY KEY)'\
                    .format(tn = name, nf = colname, ft =coltype))
    except sq.OperationalError:
        raise Exception('Table already exists. Cannot import')
    
    return

def importer(cv, conn, cur):
    """ Import csv into database as a new table
    cv is the filepath of the cv
    conn is the conenction to the database
    cur is the cursor
    """
    
    name = os.path.splitext(cv)[0]
    
    try:
        open(cv,'r')
    except FileNotFoundError:
        print('Error 404. File Not Found.')
        return
    
    with open(cv, 'r') as f:
        reader = csv.reader(f)
        i = next(reader)
        i[0] = i[0][3:]
        rest = [row for row in reader]

        # Separate to helper function
        fill = "("
        
        for x in i:
            fill = fill + "?,"
        fill = fill[:-1] + ")"

        typing = ['INTEGER' if representsInt(x) else 'TEXT' for x in rest[0]]
        
        try:
            createTable(cur,name, i[0], typing[0])
        except Exception as error:
            print(error)
            return
        
        i = i[1:]
        typing = typing[1:]
        
        for cols,types in zip(i,typing):
            cur.execute("ALTER TABLE {tn} ADD COLUMN '{nf}' '{ft}' NULL"\
                       .format(tn = name,nf = cols, ft = types))
        
        for row in rest:
            for i in range(len(row)):
                if row[i] == '':
                    row[i] = None
            comm = 'INSERT INTO '+name+' VALUES '+fill
            cur.execute(comm, row)
        
    
