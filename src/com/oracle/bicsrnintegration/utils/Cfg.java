package com.oracle.bicsrnintegration.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.oracle.property.Propertiies;
import org.oracle.property.exceptions.PropertyNotFoundException;

public class Cfg {
	private String bicsService;
	private String bicsIdentityDomain;
	private String bicsDatacenter;
	private String bicsUser;
	private String bicsPassword;
	private String rightNowUrl;
	private String rightNowUser;
	private String rightNowPassword;
	private String rightNowDelimiter;
	private String proxyServer;
	private int proxyPort;
	private int hourDifference;
	private int threads;
	private String tableFile;
	
	private Cfg(String bicsService, String bicsIdentityDomain, String bicsDatacenter, String bicsUser,
			String bicsPassword, String rightNowUrl, String rightNowUser, String rightNowPassword,
			String rightNowDelimiter, String proxyServer, int proxyPort, int hourDifference, int threads, String tableFile) {
		this.bicsService = bicsService;
		this.bicsIdentityDomain = bicsIdentityDomain;
		this.bicsDatacenter = bicsDatacenter;
		this.bicsUser = bicsUser;
		this.bicsPassword = bicsPassword;
		this.rightNowUrl = rightNowUrl;
		this.rightNowUser = rightNowUser;
		this.rightNowPassword = rightNowPassword;
		this.rightNowDelimiter = rightNowDelimiter;
		this.proxyServer = proxyServer;
		this.proxyPort = proxyPort;
		this.hourDifference = hourDifference;
		this.threads = threads;
		this.tableFile = tableFile;
	}
	
	public static Cfg fromPropertiesFile() {
	    Propertiies properties = new Propertiies();
	    try {
	    InputStream is = new FileInputStream(new File("config.properties"));

	    properties.load(is);

	    String bicsService = properties.getRequiredStringProperty("bicsService");
	    String bicsIdentityDomain = properties.getRequiredStringProperty("bicsIdentityDomain");
	    String bicsDatacenter = properties.getRequiredStringProperty("bicsDatacenter");
	    String bicsUser = properties.getRequiredStringProperty("bicsUser");
	    String bicsPassword = properties.getRequiredStringProperty("bicsPassword");
	    String rightNowUrl = properties.getRequiredStringProperty("rightNowUrl");
	    String rightNowUser = properties.getRequiredStringProperty("rightNowUser");
	    String rightNowPassword = properties.getRequiredStringProperty("rightNowPassword");
	    String rightNowDelimiter = properties.getRequiredStringProperty("rightNowDelimiter");
	    String proxyServer = properties.getRequiredStringProperty("proxyServer");
	    int proxyPort = properties.getRequiredIntProperty("proxyPort");
	    int hourDifference = properties.getRequiredIntProperty("hourDifference");
	    int threads = properties.getRequiredIntProperty("threads");
	    String tableFile = properties.getRequiredStringProperty("tableFile");
	    
	    if (proxyServer.equals("")) proxyServer = null;
	    
	    return new Cfg(bicsService, bicsIdentityDomain, bicsDatacenter, bicsUser, bicsPassword,
	    		rightNowUrl, rightNowUser, rightNowPassword, rightNowDelimiter, proxyServer, proxyPort, hourDifference, threads, tableFile);
	    } catch (IOException e) {
	    	Log.error(Module.PROPERTIES, "There was an error when trying to read file");
	    	e.printStackTrace();
	    	System.exit(1);
	    } catch (PropertyNotFoundException e) {
	    	Log.error(Module.PROPERTIES, "A required property is missing from file");
			e.printStackTrace();
	    	System.exit(1);
		}
	    
	    return null;
	}
	
	public String getBicsService() {
		return bicsService;
	}
	public String getBicsIdentityDomain() {
		return bicsIdentityDomain;
	}
	public String getBicsDatacenter() {
		return bicsDatacenter;
	}
	public String getBicsUser() {
		return bicsUser;
	}
	public String getBicsPassword() {
		return bicsPassword;
	}
	public String getRightNowUrl() {
		return rightNowUrl;
	}
	public String getRightNowUser() {
		return rightNowUser;
	}
	public String getRightNowPassword() {
		return rightNowPassword;
	}
	public String getRightNowDelimiter() {
		return rightNowDelimiter;
	}
	public String getProxyServer() {
		return proxyServer;
	}
	public int getProxyPort() {
		return proxyPort;
	}
	
	public int getHourDifference() {
		return hourDifference;
	}
	
	public int getThreads() {
		return threads;
	}
	
	public String getTableFile() {
		return tableFile;
	}
	
}
