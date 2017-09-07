# kerberized-keytab-conn-to-HMS

## Info
This repository contains sample code that could be used to connect to a kerberized (with keytab) instance of Hive Metastore Server. 

Upon successfull connection, the program uses HiveMetaStoreClient APIs to get tables along with their schemas from the specified default database.
## Instructions:
1. Download and unzip the jar.zip file
2. Run the jar as follows:
    ```
    java -jar kerberized-keytab-conn-to-HMS.jar -m {METASTORE_THRIFT_URL} -u “{USERNAME}” -p “{PRINCIPAL}” -k “{PATH_TO_KEYTAB_FILE}” -d "{DEFAULT_DATABASE_NAME}
    ```
 
    Replace {METASTORE_THRIFT_URL} with the Hive Metastore uri. ex: thrift://ip-10-11-21-34.alationdata.com:9083
    
    Replace {USERNAME} with the kerberos service account username. ex: hive/ip-10-11-21-34.alationdata.com
    
    Replace {PRINCIPAL} with the Kerberos Metastore principal. ex: “hive/_HOST@ALATION.TEST"
    
    Replace {PATH_TO_KEYTAB_FILE} with the full path to the keytab file. ex: “/etc/security/hadoop/hive.service.keytab"
    
    Replace {DEFAULT_DATABASE_NAME} with the name of the default database name to get tables from. Default value is provided as "default"
3. Optional Parameters:
   ```
   -st    {SOCKET_TIMEOUT_SECONDS} 
   -t     {TABLE_NAMES}
   -r
   -s     
   -gtobn 
 
   ```
   
   Replace {SOCKET_TIMEOUT_SECONDS} with the Hive Metastore Client socket timeout in seconds. Default is 12 seconds
   
   Replace {TABLE_NAMES} with comma separated String of table names to extract schemas of. Ex: "table_1,table_2,table_3"]

   Specify -r (Reverse table names) if table names should be reversed

   Specify -s (Skipping mechanism) if failed calls to the metastore should be skipped after retrying a default of 2 times

   Specify -gtobn (getTableObjectsByName) to invoke getTableObjectsByName method on the metastore. 

