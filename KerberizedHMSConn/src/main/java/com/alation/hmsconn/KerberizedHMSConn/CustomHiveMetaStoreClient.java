package com.alation.hmsconn.KerberizedHMSConn;

import java.io.IOException;
import java.util.logging.Logger;

import javax.security.auth.Subject;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.security.UserGroupInformation;


/**
*
* Override HiveMetaStoreClient reconnect method to use custom login context if provided
*
*/
public class CustomHiveMetaStoreClient extends HiveMetaStoreClient implements CustomIMetaStoreClient {
   final static Logger logger = Logger.getLogger(HiveMetastoreKerberizedKeytabConnection.class.getName());
   
   public CustomHiveMetaStoreClient(HiveConf conf) throws MetaException {
       super(conf);
   }

   public void reconnect(Subject loginSubject) throws IOException, MetaException  {
       if (loginSubject != null){
    	   logger.info("Setting up proper kerberos login context for the reconnect");
           // do the reconnect within the correct login context
    	   ReconnectKerbAction action = new ReconnectKerbAction((CustomIMetaStoreClient) this);
           UserGroupInformation realUgi = UserGroupInformation.getUGIFromSubject(loginSubject);
           realUgi.doAs(action);
       } else {
           super.reconnect();
       }
   }
}