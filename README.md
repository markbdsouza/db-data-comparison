# DB Data Comparison Script
The purpose of this application is to easily perform ETL based count and data validation and profile the respective source and target data quickly without any manual intervention. 
The source/target can be flat files or databases.
The tool is able to do multiple comparisons each run and the user needs to only modify the config.json file to enter the connection details for each source and the query for the database object.

## What the Script does
The script performs 2 types of validation
1. Count Validation
2. Data Validation (comparison of all columns of source and target)

The script also performs data profiling for each source and target object columns.
The below profiling is done
1. Unique Count 
2. Null Count
3. Maximum Value
4. Minimum Value
5. Mean Value

3 CSV Files are created as the output. Each having the prefix of the Test Set Name mentioned in the config.json file
All output files will be created at \OutputFiles\
1. Validation Summary
2. Profiling Results for the Source
3. Profiling Results for the Target

## Supported Sources
Currently the script is capable of handling the sources in the below 4 formats
1. TXT Files with delimiter
2. CSV Files
3. Oracle Database
4. MySQL Database 
Note: TXT and CSV Files must be placed in the \InputFiles\ Folder

## Things to be taken care of
1. The comparison is done between source and target based on column names. Hence, column names should be an exact match, otherwise all comparisons will fail
2. Confirm that you are able to connect to the databases using the credentials mentioned
3. Passwords are read from the config.json file. If your DB is accessible from another machine, delete all passwords before sharing the code with anyone else

## Modifying config.json
All modifications by the user needs to be done only in the config.json file. The script currently accepts 1 connection detail per connection type for the 4 applicable types: CSV, TXT, MySQL, Oracle
The details of the connection to these sources as well as what needs to be compared and profiled would be mentioned in the config.json file

#### CSV Config 
Accepts only filename placed in InputFiles folder. Should include the extension as well
```js
"CSV": {
    "FileName": "price-indexes-june.csv"
 }
```
#### TXT Config 
Accepts filename placed in InputFiles folder which should include the extension as well and the delimiter between columns
```js
"TXT": {
    "FileName": "data_tab_delimited.txt",
    "Delimiter": "\t"
  }
  ```
#### MySQL Config
Accepts HostName, Username, password and schema
```js
    "MySQL": {
    "HostName": "localhost",
    "UserName": "root",
    "Password": "admin",
    "Schema": "sakila"
    }
```
#### Oracle Config 
Accepts HostName, Port, ServiceName Username, Password
```js
"Oracle": {
    "HostName": "localhost",
    "Port": 1521,
    "ServiceName": "xe",
    "UserName": "system",
    "Password": "admin"
}
```
#### Running multiple queries
The value of the ComparisonQueries Key is a list of dictinoaries containing the details of the source and target 
TestSet is the name of the Tests run for the query. For CSV and TXT, the SourceQuery has no significance and should be kept empty. 
Source and Target are expected to be 1 of the 4 values - 'CSV', 'TXT', 'MySQL', 'Oracle' and will use the connection details in the file to connect to the source.
For DB related sources, the Query mentioned will be used to run on the DB to create the respective dataframe. Care must be taken to make sure the names of the columns in both source and target are the same so the tool can understand which columns need to be compared to each other
```js
"ComparisonQueries":
  [{
    "TestSet": "CSV Oracle TestSet",
    "Source": "CSV",
    "SourceQuery": "",
    "Target": "Oracle",
    "TargetQuery": "select series as \"Series reference\", period , description,revised,published from price_indexes"
  }]
 ```
