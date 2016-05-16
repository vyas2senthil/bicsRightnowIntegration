package com.oracle.bicsrnintegration;

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

	public static void log(Module module, String message) {
		String moduleStr = null;
		
		switch (module) {
		case BICS: moduleStr = "BICS"; break;
		case RIGHTNOW: moduleStr = "RIGHTNOW"; break;
		case PROPERTIES: moduleStr = "PROPERTIES"; break;
		case TABLEREADER: moduleStr = "TABLEREADER"; break;
		case INTEGRATOR: moduleStr = "INTEGRATOR"; break;
		default: moduleStr = "UNKNOWN";
		}
		
		System.out.println(getTimestamp() + " [" + moduleStr + "]: " + message);
	}
}
