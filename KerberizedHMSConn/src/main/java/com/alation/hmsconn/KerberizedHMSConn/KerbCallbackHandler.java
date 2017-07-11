package com.alation.hmsconn.KerberizedHMSConn;

import java.io.IOException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

public class KerbCallbackHandler implements CallbackHandler {
	private String _userName;

	public KerbCallbackHandler(String userName) {
		_userName = userName;
	}


	public void handle(Callback[] callbacks) throws IOException,
	UnsupportedCallbackException {
		for (Callback callback : callbacks) {
			if (callback instanceof NameCallback && _userName != null) {
				((NameCallback) callback).setName(_userName);
			}
		}
	}
}

