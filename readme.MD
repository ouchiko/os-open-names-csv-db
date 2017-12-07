# OS Open Data: Open Names CSV > SQLITE db

#### Introduction

Processes the OS Open Names data sets into SQLITE .db files.

Data can be obtained from:
https://www.ordnancesurvey.co.uk/business-and-government/products/finder.html?Licensed%20for=OpenData%20(Free)&withdrawn=on

#### Process
Download the data from ordnancesurvey.co.uk and extract the .CSV files to the ```./dataset``` directory.  

Now run the python script to process data.  The resulting .db file will be produced into ```./output``` and you can then use this as a datasource.  The database contains two tables, *postcodes* and *streets* which hold the data.

```
python ./srv/go.py
```

The resulting database is around 300MB in size and holds 2.7m records.

#### Next stages

1. Convert the easting & northing into lat/lng values.
2. Generate geo-spatial fields for this data when producing the .db file
3. Apply this data to a new revision of the postcode API
