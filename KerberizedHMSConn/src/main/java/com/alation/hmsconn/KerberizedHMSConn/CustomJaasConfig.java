package com.alation.hmsconn.KerberizedHMSConn;

import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;

public class CustomJaasConfig extends Configuration {
	private Map<String, String> customOptions;
	private final Map<String, AppConfigurationEntry[]> loginModuleConfigs = new HashMap<String, AppConfigurationEntry[]>();

	public CustomJaasConfig() {
	}

	/*
	 * Add an Entry to the jaas config (kerberos.conf)
	 * @param appName LoginContext name
	 * @param loginModuleName Login module name
	 * @param controlFlag LoginModuleControlFlag.required or LoginModuleControlFlag.optional
	 * @param options login key/value args
	 */
	public void addAppConfigurationEntry(String appName, String loginModuleName, LoginModuleControlFlag controlFlag, Map<String, String> options){
		AppConfigurationEntry[] appConfigEntries = new AppConfigurationEntry[1];
		appConfigEntries[0] = new AppConfigurationEntry(loginModuleName,
				controlFlag,
				options);
		this.loginModuleConfigs.put(appName,  appConfigEntries);
	}

	@Override
	public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
		return this.loginModuleConfigs.get(appName);
	}
}