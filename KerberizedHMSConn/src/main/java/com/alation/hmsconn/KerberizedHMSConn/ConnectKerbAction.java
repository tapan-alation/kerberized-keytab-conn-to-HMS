package com.alation.hmsconn.KerberizedHMSConn;

import javax.security.auth.Subject;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

class ConnectKerbAction implements java.security.PrivilegedAction<Void> {

	private IMetaStoreClient m_connection;
	private HiveConf m_conf;
	private Subject login_subject;
	private boolean skipLastFailedMethodCall;

	public ConnectKerbAction(final HiveConf conf, Subject subject, boolean skipLastFailedMethodCall) {
		this.m_conf = conf;
		this.login_subject = subject;
		this.skipLastFailedMethodCall = skipLastFailedMethodCall;
	}

	public IMetaStoreClient getConnection() {
		return this.m_connection;
	}

	@Override
	public Void run() {
		try {
			IMetaStoreClient client = RetryingMetaStoreClient.getProxy(m_conf,
					CustomHiveMetaStoreClient.class.getName(), this.login_subject, skipLastFailedMethodCall);
			this.m_connection = client;
		} catch (MetaException e) {
			throw new RuntimeException(e);
		}
		return null;
	}
}