package com.alation.hmsconn.KerberizedHMSConn;

import java.io.IOException;

import org.apache.hadoop.hive.metastore.api.MetaException;

public class ReconnectKerbAction implements java.security.PrivilegedAction<Void>{
	private CustomIMetaStoreClient m_client;
	public ReconnectKerbAction(CustomIMetaStoreClient client) {
		this.m_client = client;
	}

	@Override
	public Void run() {
		try {
			this.m_client.reconnect(null);
		} catch (MetaException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return null;
	}
}
