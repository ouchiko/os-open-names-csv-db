import glob
import csv
import sqlite3
import os
import os.path
import unicodecsv

from collections import OrderedDict
# ---------------------------------------------------------------------------
# Parse OS Open Data "Open Names CSV" file into Postcodes and Streets
# tab files.  These are then processed into sqlite DB files which can be used
# for address based lookups etc.
# ---------------------------------------------------------------------------
def writedbfile(dbfilename, inputfile, createstatement, insertstatement, removefile):
    ''' writes out the parsed files into sqlite db structure '''
    print " - Processing DB write %s" % (inputfile)
    if removefile and os.path.isfile(dbfilename):
        print "  - Removing DB file"
        os.remove(dbfilename)
    print "  - Connecting to SQLITE"
    con = sqlite3.connect(dbfilename)
    cur = con.cursor()
    cur.execute(createstatement)
    with open(inputfile, 'rb') as input_file:
        reader = unicodecsv.reader(input_file, delimiter="\t")
        data = [row for row in reader]
    cur.executemany(insertstatement, data)
    con.commit()
    print "  - Finished"

def loadfile(file):
    ''' Reads in csv file and processes it's content '''
    # Stack for storing the typing.
    stack = {"types": {}}
    count = 0
    with open(file, 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in reader:
            # They have a reasonably confusing data sheet presented here.
            # It consists of irregular data with different values/rules dependent
            # on the type. This process will take the data and split the contents
            # into two files, streets and postcodes.
            struct = OrderedDict()
            struct['textvalue'] = row[2]
            struct['layer'] = row[6]
            struct['type'] = row[7]
            struct['easting'] = row[8]
            struct['northing'] = row[9]
            struct['eastingmin'] = row[12]
            struct['northingmin'] = row[13]
            struct['eastingmax'] = row[14]
            struct['northingmax'] = row[15]
            struct['prepostcode'] = row[16]
            struct['address1'] = row[18]
            struct['address2'] = row[21]
            struct['address3'] = row[24]
            struct['address4'] = row[27]
            struct['address5'] = row[29]
            # Decide on where to write this row data too.
            if struct['type'] == 'Postcode':
                postcodes.write('\t'.join(struct.values()) + "\n")
            else:
                streets.write('\t'.join(struct.values()) + "\n")
            if struct['type'] not in stack['types']:
                stack['types'].update({struct['type']:0})
            stack['types'][struct['type']] += 1
            count = count + 1

        print "===================================================================================="
        print " - File: %s, Processed %d rows, Found %d types." % (file, count, len(stack['types']))
        # for i in stack['types']:
        #     print "  - %s: %d items" % (i, stack['types'][i])


# Base.
doProcess = False
if doProcess:
    i = 0
    # Read in our files for processing.
    files = glob.glob("./dataset/*.csv")
    # Open the resulting tab files.
    streets = open("output/streets.tab", "w")
    postcodes = open("output/postcodes.tab", "w")
    # Parse the files.
    while i<len(files):
        loadfile(files[i])
        i=i+1
    # Close the files.
    streets.close()
    postcodes.close()

writedbfile(
    "output/opennames.db",
    "output/postcodes.tab",
    "CREATE TABLE postcodes (textValue STRING, layer STRING, type STRING, easting INT, northing INT, eastingmin INT, northingmin INT, eastingmax INT, northingmax INT, prepostcode STRING, address1 STRING, address2 STRING, address3 STRING, address4 STRING, address5 STRING)",
    "INSERT INTO postcodes (textValue, layer, type, easting, northing, eastingmin, northingmin, eastingmax, northingmax, prepostcode, address1, address2, address3, address4, address5) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
    True
)

writedbfile(
    "output/opennames.db",
    "output/streets.tab",
    "CREATE TABLE streets (textValue STRING, layer STRING, type STRING, easting INT, northing INT, eastingmin INT, northingmin INT, eastingmax INT, northingmax INT, prepostcode STRING, address1 STRING, address2 STRING, address3 STRING, address4 STRING, address5 STRING)",
    "INSERT INTO streets (textValue, layer, type, easting, northing, eastingmin, northingmin, eastingmax, northingmax, prepostcode, address1, address2, address3, address4, address5) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
    False
)
