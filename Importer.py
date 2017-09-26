# -*- coding: utf-8 -*-
"""
Created on Fri Oct 14 15:15:45 2016

@author: Arnold Choa
"""

import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from scipy.integrate import odeint
import matplotlib.animation as animation
import math
import time
import itertools as iter
import sqlite3 as sq
import csv
import os

def representsInt(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False
    
    
def initial(dbname):
    # Establish Connection to SQL database
    # dbname is the filepath of the database
    
    conn = sq.connect(dbname+'.sqlite')
    cur = conn.cursor()
    
    return conn,cur

def close(conn):
    # Close Connection to SQL database
    # conn is the connection of the database
    # If Y, save changes. If N, do not save changes. If C, cancel closing
    
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

def importer(cv, conn, cur):
    # Import csv into database as a new table
    # cv is the filepath of the cv
    # conn is the conenction to the database
    # cur is the cursor
    
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
            cur.execute('CREATE TABLE {tn} ({nf} {ft} PRIMARY KEY)'\
                        .format(tn = name, nf = i[0], ft =typing[0]))
        except sq.OperationalError:
            print('Table already exists. Cannot import')
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
        
    
