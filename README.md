# kerberized-keytab-conn-to-HMS

## Info
This repository contains sample code that could be used to connect to a kerberized (with keytab) instance of Hive Metastore Server. 

Upon successfull connection, the program uses HiveMetaStoreClient APIs to get tables along with their schemas from the specified default database.
## Instructions:
1. Download and unzip the jar.zip file
2. Run the jar as follows:
    ```
    java -jar kerberized-keytab-conn-to-HMS.jar -m {METASTORE_THRIFT_URL} -u “{USERNAME}” -p “{PRINCIPAL}” -k “{PATH_TO_KEYTAB_FILE}”
    ```
 
    Replace {METASTORE_THRIFT_URL} with the Hive Metastore uri. ex: thrift://ip-10-11-21-34.alationdata.com:9083
    
    Replace {USERNAME} with the kerberos service account username. ex: hive/ip-10-11-21-34.alationdata.com
    
    Replace {PRINCIPAL} with the Kerberos Metastore principal. ex: “hive/_HOST@ALATION.TEST"
    
    Replace {PATH_TO_KEYTAB_FILE} with the full path to the keytab file. ex: “/etc/security/hadoop/hive.service.keytab"
