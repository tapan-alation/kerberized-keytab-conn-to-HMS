package com.alation.hmsconn.KerberizedHMSConn;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;

/**
 *
 * Modified from https://raw.githubusercontent.com/apache/hive/master/metastore/src/java/org/apache/hadoop/hive/metastore/RetryingMetaStoreClient.java
 *
 */
public class RetryingMetaStoreClient implements InvocationHandler{
	private static final Log LOG = LogFactory.getLog(RetryingMetaStoreClient.class.getName());

	private final CustomIMetaStoreClient base;
	private final int retryLimit = 10;
	private final long retryDelaySeconds = 2;
	private final Subject loginSubject;

	public RetryingMetaStoreClient(HiveConf hiveConf,
			Class<? extends CustomIMetaStoreClient> msClientClass,
			Subject subject) throws MetaException {
		this.loginSubject = subject;
		this.base = MetaStoreUtils.newInstance(msClientClass, new Class[] {HiveConf.class},
				new Object[] {hiveConf});

	}

	public static CustomIMetaStoreClient getProxy(HiveConf hiveConf,
			String mscClassName, Subject loginSubject) throws MetaException {

		Class<? extends CustomIMetaStoreClient> baseClass = (Class<? extends CustomIMetaStoreClient>) MetaStoreUtils.getClass(mscClassName);

		RetryingMetaStoreClient handler = new RetryingMetaStoreClient(hiveConf, baseClass, loginSubject);

		return (CustomIMetaStoreClient) Proxy.newProxyInstance(RetryingMetaStoreClient.class.getClassLoader(), baseClass.getInterfaces(), handler);
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		Object ret = null;
		int retriesMade = 0;
		TException caughtException = null;

		while (true) {
			try {
				if(retriesMade > 0){
					// reconnect within correct login context if subject provided
					base.reconnect(this.loginSubject);
				}
				ret = method.invoke(base, args);
				break;
			} catch (UndeclaredThrowableException e) {
				throw e.getCause();
			} catch (InvocationTargetException e) {
				Throwable t = e.getCause();
				if (t instanceof TApplicationException){
					TApplicationException tae = (TApplicationException)t;
					switch (tae.getType()) {
					case TApplicationException.MISSING_RESULT:
					case TApplicationException.UNKNOWN_METHOD:
					case TApplicationException.WRONG_METHOD_NAME:
					case TApplicationException.PROTOCOL_ERROR:
						throw t;
					default:
						// TODO: most other options are probably unrecoverable... throw?
								caughtException = tae;
					}
				} else if ((t instanceof TProtocolException) || (t instanceof TTransportException)) {
					// TODO: most protocol exceptions are probably unrecoverable... throw?
					caughtException = (TException)t;
				} else if ((t instanceof MetaException) && t.getMessage().matches("(?s).*(JDO[a-zA-Z]*|TProtocol|TTransport)Exception.*")
						&& !t.getMessage().contains("java.sql.SQLIntegrityConstraintViolationException")) {
					caughtException = (MetaException)t;
				} else {
					throw t;
				}
			} catch (MetaException e) {
				if (e.getMessage().matches("(?s).*(IO|TTransport)Exception.*")
						&& !e.getMessage().contains("java.sql.SQLIntegrityConstraintViolationException")) {
					caughtException = e;
				} else {
					throw e;
				}
			}

			if (retriesMade >=  retryLimit) {
				throw caughtException;
			}
			retriesMade++;
			LOG.warn("MetaStoreClient lost connection. Attempting to reconnect (" + retriesMade +
					" of " + retryLimit + ") after " + retryDelaySeconds + "s. " + method.getName(), caughtException);
			Thread.sleep(retryDelaySeconds * 1000);
		}
		return ret;
	}
}