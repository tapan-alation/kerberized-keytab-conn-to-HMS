package com.alation.hmsconn.KerberizedHMSConn;

import java.io.IOException;

import javax.security.auth.Subject;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

public interface CustomIMetaStoreClient extends IMetaStoreClient {
	/**
	 *  Tries to reconnect this MetaStoreClient to the MetaStore
	 *      within correct security context if applicable
	 * @throws IOException
	 */
	void reconnect(Subject subject) throws MetaException, IOException;
}
