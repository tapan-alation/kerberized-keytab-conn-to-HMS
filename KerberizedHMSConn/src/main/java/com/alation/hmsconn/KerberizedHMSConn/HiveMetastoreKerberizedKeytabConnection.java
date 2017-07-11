package com.alation.hmsconn.KerberizedHMSConn;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.thrift.TException;


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
	static int metastoreClientSocketTimeout = 1200;
	static int metastoreClientRetryOnError = 10;
	static HiveConf hiveConf; 

	public static void main(String[] args)
	{
		parseCommandLineArgs(args);
		setupHiveConf();
		IMetaStoreClient conn = createInitialHMSConnection();
		getTablesAndSchemas(conn);
	}

	private static void parseCommandLineArgs(String[] args)
	{
		Options options = new Options();
		options.addOption("m", "metastoreuri", true, "Set the Metastore URI");
		options.addOption("u", "username", true, "Set the Username");
		options.addOption("p", "kerberosprincipal", true, "Set the Kerberos Principal");
		options.addOption("k", "keytabpath", true, "Set the Keytab path");
		options.addOption("d", "defaultdatabase", true, "Set the default database");
		options.addOption("r", "retryonerror", true, "Set the METASTORETHRIFTFAILURERETRIES. Default is 10 times");
		options.addOption("s", "sockettimeout", true, "Set the Hive Metastore Client socket timeout in seconds. Default is 1200 seconds");
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

			if (cmdLine.hasOption("d")){
				logger.log(Level.INFO, "Using default database to connect to on the Hive Metastore Server as: " + cmdLine.getOptionValue("d"));
				defaultDatabase = cmdLine.getOptionValue("d");
			}

			if (cmdLine.hasOption("r")){
				logger.log(Level.INFO, "Using retryonerror setting as: " + cmdLine.getOptionValue("r"));
				metastoreClientSocketTimeout = Integer.parseInt(cmdLine.getOptionValue("r"));
			}

			if (cmdLine.hasOption("s")){
				logger.log(Level.INFO, "Using socket timeout to Hive Metastore Server as: " + cmdLine.getOptionValue("s"));
				metastoreClientSocketTimeout = Integer.parseInt(cmdLine.getOptionValue("s"));
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
			action = new ConnectKerbAction(hiveConf, lc.getSubject());
			UserGroupInformation realUgi = UserGroupInformation.getUGIFromSubject(lc.getSubject());
			realUgi.doAs(action);
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Error with Kerberos Authentication!", e);
		}
		return action.getConnection();
	}

	private static void getTablesAndSchemas(IMetaStoreClient metaStoreClient)
	{
		try {
			List<String> tables = null;
			String db = defaultDatabase;
			logger.log(Level.INFO, new java.util.Date() + "About to get all table names for db: " + db);
			tables = metaStoreClient.getAllTables(db);

			logger.log(Level.INFO, new java.util.Date() + "------------Table names (Found " + Integer.toString(tables.size()) + " tables)----------------");
			logger.log(Level.INFO, Arrays.toString(tables.toArray()));
			logger.log(Level.INFO, "------------");
			for (String tableName: tables) { 
				List<FieldSchema> fields = null;
				try {
					logger.log(Level.INFO, new java.util.Date() + "About to get all field names / table schema for table: " + tableName);
					fields = metaStoreClient.getSchema(db, tableName);
				} catch (Exception e) {
					logger.log(Level.SEVERE, "Error getting schema for table: " + tableName, e);
				}

				if (fields != null) {
					List<FieldSchema> fieldResults = new ArrayList<FieldSchema>();
					for (FieldSchema field : fields) {
						fieldResults.add(new FieldSchema(field));
					}
					logger.log(Level.INFO, new java.util.Date() + "------------Fields for table: " + tableName + " (Found" + Integer.toString(fieldResults.size()) + " fields)----------------");
					logger.log(Level.INFO, Arrays.toString(fieldResults.toArray()));
					logger.log(Level.INFO, "------------");
				}
			}
			logger.log(Level.INFO, "Done!");
			System.exit(0);
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
}