package com.oracle.bicsrnintegration.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Log {
	private static String getTimestamp() {
		Calendar c = Calendar.getInstance();
		Date d = c.getTime();
		SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
		return sdf.format(d);
	}

	public static void info(Module module, String message) {
		System.out.println("(i) " + getTimestamp() + " [" + getModuleDescription(module) + "]: " + message);
	}
	
	public static void warning(Module module, String message) {
		System.out.println("(!) " + getTimestamp() + " [" + getModuleDescription(module) + "]: " + message);
	}
	
	public static void error(Module module, String message) {
		System.err.println("(X) " + getTimestamp() + " [" + getModuleDescription(module) + "]: " + message);
	}
	
	private static String getModuleDescription(Module module) {
		switch (module) {
		case RIGHTNOW: return "RightNow";
		case BICS: return "BICS";
		case TABLEREADER: return "TableFileReader";
		case INTEGRATOR: return "Integrator";
		case PROPERTIES: return "PropertiesFileReader";
		case BICSOUTGESTERMANAGER: return "BicsOutgesterManager";
		case BICSOUTGESTERWORKER: return "BicsOutgesterWorker";
		default: return "Unknown";
		}
	}
}
