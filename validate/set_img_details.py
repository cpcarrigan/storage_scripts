#!/usr/bin/python3

import sys
from pymongo import MongoClient
from pprint import pprint
import gzip
import json



def querymongo ( file ):
    global k

    myquery = { "dataCenter" : region, "ownerAccountId" : accid, "_id" : assid, "files.fileType" :  "HIRES"  }
    mydoc = mycol.find(myquery,filters)
    for x in mydoc:
        k = x
        #pprint(x)

#------------------------------------------------

def testjournal ( j ):
    global jc

    if "journalCore" in json.dumps(j):
    #if "journalDeveloper" in json.dumps(j):
        jc = True
    else:
        jc = False

#------------------------------------------------

def updatehws ( update ):
    print("running hws")
    myquery = { "dataCenter" : region, "ownerAccountId" : accid, "_id" : assid, "files.fileType" :  "HIRES"  }
    newvalues = { "$set" : { "files.$.height" : fheight, "files.$.width" : fwidth, "files.$.size" : fsize } }
    mycol.update_one(myquery, newvalues)
    mydoc = mycol.find(myquery,filters)

    for x in mydoc:
        print("update h w s only")
        #pprint(x)

#--------------------------------------------------

def updatej ( update ):
    print("running j")
    myquery = { "dataCenter" : region, "ownerAccountId" : accid, "_id" : assid, "systemTags.key" : "journalCore" }
    fmyquery = { "dataCenter" : region, "ownerAccountId" : accid, "_id" : assid }
    newvalues = { "$set" : { "systemTags.$.key" : "journalCoreBackup" } }
    mycol.update_one(myquery, newvalues)
    mydoc = mycol.find(fmyquery,filters)
    for x in mydoc:
        print("update j only")
        #pprint(x)

#----------------------------------------------------

#This will open the csv.gz file and store it as f
with gzip.open(sys.argv[1],'rb') as f:

#This will loop through the file line by line
  for line in f:

#This prints the line for easy viewing, Can hash out if not needed
#    print(line)

#This stores the loop variable into specific variables
    #Production asset mongoDB
    #client = MongoClient('mongodb://assets.tnt11-zone2.phl1:27017/', username='sfastapp1', password='R5nPSC2pbk')
    #AWS QA mongoDB
    client = MongoClient('mongodb+srv://snapfish-qa1.tac3y.mongodb.net/', username='sfastapp1', password='sfqa')
    db = client ['SNAPFISH_Asset']
    mycol = db['SNAPFISH_Asset']
    filters = { "files": 1, "systemTags": 1 }
    file_content=line.split(',')
    assid = int(file_content[0])
    accid = int(file_content[1])
    region = file_content[2]
    status = file_content[5]
    fwidth = int(file_content[7])
    fheight = int(file_content[8])
    fsize = int(file_content[9])

#Initial query to get the record
    querymongo(f)

#Check to see if its OLR or HR, if true just update otherwise update with journal
    if "HR" in status or "OLR" in status:
        print("HR and OLR exist")
        updatehws(k)
    else:
        print("Need to check for journalCore")
        testjournal(k)
        if jc:
            print("journalcore exist? " + str(jc) )
            updatehws(k)
            updatej(k)
        else:
            print("journalcore exist? " + str(jc) )
            updatehws(k)


    pprint(k)
