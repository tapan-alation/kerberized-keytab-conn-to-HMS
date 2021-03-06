package com.alation.hmsconn.KerberizedHMSConn;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.thrift.TException;

import com.google.common.base.Strings;


/**
 * Create a kerberized connection using keytab to Hive Metastore Server
 * Get all tables on a specified database.
 * 	For each table, also get the table schema.
 *
 */
public class HiveMetastoreKerberizedKeytabConnection 
{
	final static Logger logger = Logger.getLogger(HiveMetastoreKerberizedKeytabConnection.class.getName());
	static String metastoreUri;
	static String username;
	static String kerberosPrincipal;
	static String keytabPath;
	static String defaultDatabase = "default";
	static int metastoreClientSocketTimeout = 12;
	static HiveConf hiveConf;
	static String tablesToExtract = "";
	static boolean getTablesInReverse = false;
	static boolean use_getTableObjectsByName_call = false;
	static boolean use_getDatabases_call = false;

	static ArrayList<String> tablesWithExceptions = new ArrayList<String>();
	static ArrayList<String> tablesSkipped = new ArrayList<String>();

	public static void main(String[] args)
	{
		parseCommandLineArgs(args);
		setupHiveConf();
		IMetaStoreClient conn = createInitialHMSConnection();
		if (!Strings.isNullOrEmpty(tablesToExtract)) {
			getSchemasForGivenTables(conn, defaultDatabase, tablesToExtract.split(","));
		} else if (use_getDatabases_call) {
			getAllDatabases(conn);
		}
		else {
			getAllTablesAndSchemas(conn, defaultDatabase);
		}
		printStats();
	}

	private static void getAllDatabases(IMetaStoreClient conn) {
		List<String> databases = null;
		logger.log(Level.INFO,"About to get all database names by calling getDatabases ");
		try {
            databases = conn.getAllDatabases();
            logger.log(Level.INFO,"------------Database names (Found: " + Integer.toString(databases.size()) + " databases)----------------");
            logger.log(Level.INFO, Arrays.toString(databases.toArray()));
            logger.log(Level.INFO, "------------Now will try to call getDatabase on each database name--------");
            for (String databaseName: databases) {
                Database hivedb = null;
                logger.log(Level.INFO, "------------Calling getDatabase for database name: " + databaseName);
                hivedb = conn.getDatabase(databaseName);
                logger.log(Level.INFO, hivedb.getName() + " description: " + hivedb.getDescription() +
                        " Location URI: " + hivedb.getLocationUri());
            }

        } catch (Exception e){
		    logger.log(Level.SEVERE, "Caught an error: " + e.getMessage() + ". Stack trace: ");
            e.printStackTrace();
        }
	}

	private static void parseCommandLineArgs(String[] args)
	{
		Options options = new Options();
		options.addOption("m", "metastoreuri", true, "Set the Metastore URI");
		options.addOption("u", "username", true, "Set the Username");
		options.addOption("p", "kerberosprincipal", true, "Set the Kerberos Principal");
		options.addOption("k", "keytabpath", true, "Set the Keytab path");
		options.addOption("d", "defaultdatabase", true, "Set the default database");
		options.addOption("r", "reverseTables", false, "When set, tables are extracted in reverse order");
		options.addOption("st", "sockettimeout", true, "Set the Hive Metastore Client socket timeout in seconds. Default is 12 seconds");
		options.addOption("t", "tablesToExtract", true, "Specify a comma separated list of table names to extract schemas of. ");
		options.addOption("gtobn", "use_getTableObjectsByName_call", false, "Specify if getTableObjectsByName method of the metastore client should be invoked");
		options.addOption("dbs", "get_all_databases_or_schemas", false, "Specify if getAllDatabases and getDatabase method of the metastore client should be invoked");
		CommandLineParser parser = new BasicParser();
		CommandLine cmdLine = null;
		try {
			cmdLine = parser.parse(options,  args);
			if (cmdLine.hasOption("m")){
				logger.log(Level.INFO, "Using metastore uri: " + cmdLine.getOptionValue("m"));
				metastoreUri = cmdLine.getOptionValue("m");
			} else {
				logger.log(Level.SEVERE, "Missing metastore uri, -m option");
			}

			if (cmdLine.hasOption("u")){
				logger.log(Level.INFO, "Using username: " + cmdLine.getOptionValue("u"));
				username = cmdLine.getOptionValue("u");
			} else {
				logger.log(Level.SEVERE, "Missing username, -u option");
			}

			if (cmdLine.hasOption("p")){
				logger.log(Level.INFO, "Using kerberos principal: " + cmdLine.getOptionValue("p"));
				kerberosPrincipal = cmdLine.getOptionValue("p");
			} else {
				logger.log(Level.SEVERE, "Missing kerberos principal, -p option");
			}

			if (cmdLine.hasOption("k")){
				logger.log(Level.INFO, "Using keytab path: " + cmdLine.getOptionValue("k"));
				keytabPath = cmdLine.getOptionValue("k");
			} else {
				logger.log(Level.SEVERE, "Missing keytab path, -k option");
			}

			if (cmdLine.hasOption("r")){
				logger.log(Level.INFO, "Will get tables in reverse order");
				getTablesInReverse = true;
			}

			if (cmdLine.hasOption("gtobn")) {
                logger.log(Level.INFO, "Will use getTableObjectsByName method of the metastore client");
                use_getTableObjectsByName_call = true;
            }

			if (cmdLine.hasOption("dbs")) {
				logger.log(Level.INFO, "Will use getDatabase and getDatabases method of the metastore client");
				use_getDatabases_call = true;
			}

			if (cmdLine.hasOption("d")){
                logger.log(Level.INFO, "Using default database to connect to on the Hive Metastore Server as: " + cmdLine.getOptionValue("d"));
                defaultDatabase = cmdLine.getOptionValue("d");
            }

			if (cmdLine.hasOption("st")){
				logger.log(Level.INFO, "Using socket timeout to Hive Metastore Server as: " + cmdLine.getOptionValue("st"));
				metastoreClientSocketTimeout = Integer.parseInt(cmdLine.getOptionValue("st"));
			}

			if (cmdLine.hasOption("t")){
				logger.log(Level.INFO, "Will extract schemas for only the following tables: " + cmdLine.getOptionValue("t"));
				tablesToExtract = cmdLine.getOptionValue("t");
			}

		} catch (ParseException e) {
			logger.log(Level.SEVERE, "Failed to parse command line properties", e);
		}
	}

	private static void setupHiveConf() 
	{
		HiveConf conf = new HiveConf();
		conf.setVar(HiveConf.ConfVars.METASTOREURIS, metastoreUri);
		// Set timeout for socket read() operations to 20 minutes.
		// Refer to below URL for more information
		// http://docs.oracle.com/javase/7/docs/api/java/net/Socket.html#setSoTimeout(int)
		conf.setIntVar(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, metastoreClientSocketTimeout);

		String password = "";
		conf.set("user", username);
		if (KerberosName.getRules() == null) {
			KerberosName.setRules("DEFAULT") ;
		}

		conf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, true);
		conf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL, kerberosPrincipal);
		hiveConf = conf;
	}

	private static IMetaStoreClient createInitialHMSConnection() {
		LoginContext lc = null;
		String loginContextName = "primaryLoginContext";

		String kerberosLoginContextName = loginContextName + keytabPath;
		KerbCallbackHandler kcbh = new KerbCallbackHandler(username);
		Map<String, String> customOpts = new HashMap<String, String>();
		customOpts.put("refreshKrb5Config", "true");
		customOpts.put("useTicketCache", "false");
		customOpts.put("useKeyTab", "true");
		customOpts.put("keyTab", keytabPath);
		customOpts.put("debug", "true");
		CustomJaasConfig customJaasConfig = new CustomJaasConfig();
		customJaasConfig.addAppConfigurationEntry(kerberosLoginContextName, "com.sun.security.auth.module.Krb5LoginModule", LoginModuleControlFlag.REQUIRED, customOpts);
		ConnectKerbAction action = null;
		try {
			lc = new LoginContext(kerberosLoginContextName, null, kcbh, customJaasConfig);
			lc.login();
			action = new ConnectKerbAction(hiveConf, lc.getSubject(), true);
			UserGroupInformation realUgi = UserGroupInformation.getUGIFromSubject(lc.getSubject());
			realUgi.doAs(action);
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Error with Kerberos Authentication!", e);
		}
		return action.getConnection();
	}

	private static String convertExceptionToString(Exception e) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		return sw.toString(); // stack trace as a string
	}

	private static void getTableObjectsByName(IMetaStoreClient metaStoreClient, String db, List<String> tables) {
	    try {
	        List<Table> tableResults  = metaStoreClient.getTableObjectsByName(db, tables);
	        for (Table tableResult : tableResults){
	            logger.log(Level.INFO, "---------Got Table: " + tableResult.getTableName() + " in db: " + tableResult.getDbName());
	        }
	    } catch (Exception e) {
	        throw new RuntimeException(e);
	    }
	}

	private static void getSchemasForGivenTables(IMetaStoreClient metaStoreClient, String db, String[] tables) {
		try {
			for (String tableName: tables) {
				List<FieldSchema> fields = null;
				try {
					logger.log(Level.INFO, "About to get all field names / table schema for table: " + tableName);
					fields = metaStoreClient.getSchema(db, tableName);
				} catch (Exception e) {
					logger.log(Level.SEVERE, "Error getting schema for table: " + tableName, e);
					tablesWithExceptions.add(tableName + ": \n " + convertExceptionToString(e));
					continue;
				}

				if (null != fields) {
					List<FieldSchema> fieldResults = new ArrayList<FieldSchema>();
					for (FieldSchema field : fields) {
						fieldResults.add(new FieldSchema(field));
					}
					logger.log(Level.INFO,"------------Fields for table: " + tableName + " (Found: " + Integer.toString(fieldResults.size()) + " fields)----------------");
					logger.log(Level.INFO, Arrays.toString(fieldResults.toArray()));
					logger.log(Level.INFO, "------------");
				} else {
					logger.log(Level.INFO, "Skipped table: " + tableName);
					tablesSkipped.add(tableName);
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static void getAllTablesAndSchemas(IMetaStoreClient metaStoreClient, String db)
	{
		try {
			List<String> tables = null;
			logger.log(Level.INFO,"About to get all table names for db: " + db);
			tables = metaStoreClient.getAllTables(db);
			if (getTablesInReverse) {
                Collections.reverse(tables);
            }

            logger.log(Level.INFO,"------------Table names (Found: " + Integer.toString(tables.size()) + " tables)----------------");
            logger.log(Level.INFO, Arrays.toString(tables.toArray()));
            logger.log(Level.INFO, "------------");

			if (use_getTableObjectsByName_call) {
			    getTableObjectsByName(metaStoreClient, db, tables);
			} else {
	            String[] tblsarray = tables.toArray(new String[0]);
	            getSchemasForGivenTables(metaStoreClient, db, tblsarray);
			}
		} catch (MetaException e) {
			throw new RuntimeException(e);
		} catch (UnknownDBException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (TException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	private static void printStats() {
		int numTablesWithExceptions = tablesWithExceptions.size();
		int numTablesSkipped = tablesSkipped.size();
		if (numTablesWithExceptions > 0) {
			logger.log(Level.INFO, "Number of tables with exceptions: " + numTablesWithExceptions);
			logger.log(Level.INFO, "Tables with exceptions: ");
			for (String tableNameWithException : tablesWithExceptions) {
				logger.log(Level.INFO, tableNameWithException);
				logger.log(Level.INFO, "--------");
			}
		}
		if (numTablesSkipped > 0) {
			logger.log(Level.INFO, "Number of tables skipped due to reconnection to the metastore: " + numTablesSkipped);
			logger.log(Level.INFO, "Skipped Table Names due to lack of response from  Hive MetaStore Server: ");
			for (String skippedTableName : tablesSkipped) {
				logger.log(Level.INFO, skippedTableName);
			}
		}
		logger.log(Level.INFO, "Done!");
	}
}
