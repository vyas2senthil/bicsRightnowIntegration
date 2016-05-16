package com.oracle.bicsrnintegration;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;

import com.oracle.bics.BicsConnection;
import com.oracle.bics.apis.table.TableApi;
import com.oracle.bics.apis.table.objects.Table;
import com.oracle.http.DefaultResponse;
import com.oracle.rightnow.RightNowClient;

public class Main {

	private static TableApi tableApi;

	public static void main(String[] args) throws InterruptedException {
		Cfg cfg = Cfg.fromPropertiesFile();
		
		RightNowClient rn = new RightNowClient(cfg.getRightNowUrl(), cfg.getRightNowUser(), cfg.getRightNowPassword(), 
				cfg.getRightNowDelimiter(), cfg.getProxyServer(), cfg.getProxyPort());

		BicsConnection bics = new BicsConnection(cfg.getBicsService(), cfg.getBicsIdentityDomain(), cfg.getBicsDatacenter(),
				cfg.getBicsUser(), cfg.getBicsPassword(), cfg.getProxyServer(), cfg.getProxyPort());

		tableApi = new TableApi(bics);
		
		Calendar past = Calendar.getInstance(); past.set(1970, 1, 1, 0, 0, 1);
		String minUpdatedTime = RightNowClient.getTimestamp(past);

		Calendar now = Calendar.getInstance(); now.add(Calendar.HOUR, cfg.getHourDifference());
		String maxUpdatedTime = RightNowClient.getTimestamp(now);

		
		Log.log(Module.TABLEREADER, "Reading table file...");
		TableMapping tableMapping = TableMapping.read(new File(cfg.getTableFile()));
		Log.log(Module.TABLEREADER, "Working with RightNow table '" + tableMapping.getRightNowTableName() + "' and its " + tableMapping.getRightNowColumns().size() + " columns.");
		
		Log.log(Module.INTEGRATOR, "Dropping table '" + tableMapping.getTableName() + "' for testing purposes");
		tableApi.deleteTable(tableMapping.getTableName());
		Log.log(Module.INTEGRATOR, "Table dropped!");
		
		tableSetup(tableMapping.getTableName(), tableMapping.getTable());
		
		Log.log(Module.RIGHTNOW, "Getting MIN(ID) and MAX(ID) from '" + tableMapping.getRightNowTableName() + "'...");
		String[] minMax = rn.execQuery("USE REPORT; SELECT MIN(ID), MAX(ID) FROM " + tableMapping.getRightNowTableName() + " WHERE updatedtime &gt; '" + 
				minUpdatedTime + "' AND updatedtime &lt; '" + maxUpdatedTime + "'", 1).getResponse()
				.split("\\r\\n")[1].split(cfg.getRightNowDelimiter());
		Log.log(Module.RIGHTNOW,  "MIN(ID) = " + minMax[0] + " and MAX(ID) = " + minMax[1]);

		int minId = Integer.valueOf(minMax[0]), maxId = Integer.valueOf(minMax[1]);

		Log.log(Module.INTEGRATOR, "Initializing threads...");
		int bucketSize = (maxId - minId + 1) / cfg.getThreads();
		ArrayList<Thread> threads = new ArrayList<Thread>();
		for (int i = 0; i < cfg.getThreads(); i++) {
			int bucketMinId = minId + (bucketSize * i);
			int bucketMaxId = i < (cfg.getThreads() - 1) ? ((minId + (bucketSize * (i + 1))) - 1) : maxId;
			RightNowQueryWorker rnqw = new RightNowQueryWorker(i, bucketMinId, bucketMaxId, minUpdatedTime, maxUpdatedTime, tableMapping.getRightNowColumns(), tableMapping.getRightNowTableName(), rn);
			threads.add(new Thread(rnqw));
			threads.get(i).start();
		}
		
		for (Thread thread : threads)
			thread.join();
		
		Log.log(Module.INTEGRATOR, "All records from table '" + tableMapping.getRightNowTableName() + "' were fetched!");

	}

	public static void tableSetup(String tableName, Table t) {
		Log.log(Module.BICS, "Checking if table '" + tableName + "' exists...");
		if (!tableExists(tableName)) {
			Log.log(Module.BICS,  "It does not exist.");
			createTable(tableName, t);
		} else {
			Log.log(Module.BICS,  "It exists. Nothing to do.");
		}
	}

	public static boolean tableExists(String tableName) {
		DefaultResponse dr = tableApi.getTableInfo(tableName.toUpperCase());

		if (dr.getContent().equals("[BICS-DATALOAD] There is no table by the name " + tableName))
			return false;

		return true;
	}

	public static void createTable(String tableName, Table t) {
		Log.log(Module.BICS, "Creating table '" + tableName + "'...");
		DefaultResponse dr = tableApi.createTable(tableName, t);
		Log.log(Module.BICS, dr.getContent() + " (Status Code: " + dr.getCode() + ")");
		if (dr.getCode() != 200) {
			Log.log(Module.BICS, "It was not possible to create the table");
			System.exit(1);
		}
	}
}
