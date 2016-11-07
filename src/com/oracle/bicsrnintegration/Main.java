package com.oracle.bicsrnintegration;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;

import com.oracle.bics.BicsConnection;
import com.oracle.bics.apis.cache.CacheApi;
import com.oracle.bics.apis.table.TableApi;
import com.oracle.bics.apis.table.objects.Table;
import com.oracle.bics.apis.tabledata.objects.DataloadColumnMap;
import com.oracle.bics.apis.tabledata.objects.TableData;
import com.oracle.bics.apis.tabledata.objects.TableDataColumn;
import com.oracle.bics.common.enums.WriteMode;
import com.oracle.bicsrnintegration.objects.BicsTableDescription;
import com.oracle.bicsrnintegration.outgester.BicsOutgesterManager;
import com.oracle.bicsrnintegration.utils.Cfg;
import com.oracle.bicsrnintegration.utils.Log;
import com.oracle.bicsrnintegration.utils.Module;
import com.oracle.http.DefaultResponse;
import com.oracle.rightnow.RightNowClient;
import com.oracle.rightnow.RightNowResponse;

public class Main {

	private static TableApi tableApi;
	private static Cfg cfg;

	public static void main(String[] args) throws InterruptedException {
		cfg = Cfg.fromPropertiesFile();
		
		/* CREDENTIALS */
		RightNowClient rn = new RightNowClient(cfg.getRightNowUrl(), cfg.getRightNowUser(), cfg.getRightNowPassword(), 
				cfg.getRightNowDelimiter(), cfg.getProxyServer(), cfg.getProxyPort());

		BicsConnection bics = new BicsConnection(cfg.getBicsService(), cfg.getBicsIdentityDomain(), cfg.getBicsDatacenter(),
				cfg.getBicsUser(), cfg.getBicsPassword(), cfg.getProxyServer(), cfg.getProxyPort());

		/* API */
		tableApi = new TableApi(bics);
		
		/* DATETIME */
		Calendar past = Calendar.getInstance(); past.set(1970, 1, 1, 0, 0, 1);
		String minUpdatedTime = RightNowClient.getTimestamp(past);

		Calendar now = Calendar.getInstance(); now.add(Calendar.HOUR, cfg.getHourDifference());
		String maxUpdatedTime = RightNowClient.getTimestamp(now);

		/* READING TABLE DESCRIPTION FROM FILE */
		Log.info(Module.TABLEREADER, "Reading table file...");
		TableDescription tableDescription = TableDescription.read(new File(cfg.getTableFile()));
		BicsTableDescription bicsTableDescription = new BicsTableDescription(bics, tableDescription.getTableName(), createTableData(tableDescription.getTable()));
		Log.info(Module.TABLEREADER, "Working with RightNow table '" + tableDescription.getRightNowTableName() + "' and its " + tableDescription.getRightNowColumns().size() + " columns.");
		
		/* DROPPING EXISTING TABLE : FOR TEST ONLY! */
		Log.info(Module.BICS, "Dropping table '" + tableDescription.getTableName() + "' for testing purposes");
		DefaultResponse dr = tableApi.deleteTable(tableDescription.getTableName());
		if (dr.getCode() != 200) {
			Log.error(Module.BICS, "Error when trying to drop table ('" + dr.getContent() + "').");
			System.exit(1);
		}
		Log.info(Module.BICS, "Table dropped!");
		
		/* CREATING TABLE IF NOT EXISTS */
		createBicsTableIfNotExists(tableDescription.getTableName(), tableDescription.getTable());
		
		/* CREATING BICS_OUTGESTER_MANAGER */
		BicsOutgesterManager bicsOutgesterManager = new BicsOutgesterManager(bicsTableDescription);
		Thread outgesterManager = new Thread(bicsOutgesterManager);
		outgesterManager.start();
		
		/* GETTING COUNT OF OBJECT FROM RIGHTNOW */
		Log.info(Module.RIGHTNOW, "Getting record count from '" + tableDescription.getRightNowTableName() + "'...");
		RightNowResponse rightNowResponse = rn.execQuery("USE REPORT; SELECT COUNT(*) FROM " + tableDescription.getRightNowTableName(), 1);
		String recordCount = null;
		if (rightNowResponse.isValid())
			recordCount = rightNowResponse.getResponse();
		else {
			Log.error(Module.RIGHTNOW, rightNowResponse.getResponse());
			System.exit(1);
		}
		
		Log.info(Module.RIGHTNOW, "There are " + recordCount.split("\\r\\n")[1] + " records in the table");
		
		/* GETTING MIN(ID) AND MAX(ID) FROM RIGHTNOW */
		Log.info(Module.RIGHTNOW, "Getting MIN(ID) and MAX(ID) from '" + tableDescription.getRightNowTableName() + "'...");
		String[] minMax = rn.execQuery("USE REPORT; SELECT MIN(ID), MAX(ID) FROM " + tableDescription.getRightNowTableName() + " WHERE updatedtime &gt; '" + 
				minUpdatedTime + "' AND updatedtime &lt; '" + maxUpdatedTime + "'", 1).getResponse()
				.split("\\r\\n")[1].split(cfg.getRightNowDelimiter());
		Log.info(Module.RIGHTNOW,  "MIN(ID) = " + minMax[0] + " and MAX(ID) = " + minMax[1]);
		int minId = Integer.valueOf(minMax[0]), maxId = Integer.valueOf(minMax[1]);

		/* INITIALIZING ACTUAL THREADS THAT WILL WORK WITH RIGHTNOW DATA */
		Log.info(Module.INTEGRATOR, "Initializing threads...");
		int bucketSize = (maxId - minId + 1) / cfg.getThreads();
		ArrayList<Thread> threads = new ArrayList<Thread>();
		for (int i = 0; i < cfg.getThreads(); i++) {
			int bucketMinId = minId + (bucketSize * i);
			int bucketMaxId = i < (cfg.getThreads() - 1) ? ((minId + (bucketSize * (i + 1))) - 1) : maxId;
			RightNowQueryWorker rnqw = new RightNowQueryWorker(i + 1, bucketMinId, bucketMaxId, minUpdatedTime,
					maxUpdatedTime, tableDescription.getRightNowColumns(), tableDescription.getRightNowTableName(), rn, bicsOutgesterManager);
			threads.add(new Thread(rnqw));
			threads.get(i).start();
		}
		
		/* WAIT FOR ALL RIGHTNOW THREADS TO BE GONE */
		for (Thread thread : threads) thread.join();
		
		/* STOP OUTGESTERMANAGER */
		bicsOutgesterManager.setDone();
		Log.info(Module.BICSOUTGESTERMANAGER, "Waiting for outgesterManager to finish processing...");
		outgesterManager.join();
		Log.info(Module.BICSOUTGESTERMANAGER, "Done!");
		
		Log.info(Module.INTEGRATOR, "All records from table '" + tableDescription.getRightNowTableName() + "' were fetched!");
		
		/* DELETING CACHE */
		Log.info(Module.BICS, "Deleting cache for table '" + tableDescription.getTableName());
		CacheApi cacheApi = new CacheApi(bics);
		dr = cacheApi.deleteDataCachedForAllTablesOnAllDatabases();
		if (dr.getCode() == 200)
			Log.info(Module.BICS, "Cache was deleted!");
		else {
			Log.error(Module.BICS, "Error happened when trying to delete this cache (" + dr.getCode() + ").");
			System.exit(1);
		}
		

	}

	private static void createBicsTableIfNotExists(String tableName, Table t) {
		Log.info(Module.BICS, "Checking if table '" + tableName + "' exists...");
		if (!tableExists(tableName)) {
			Log.warning(Module.BICS,  "It does not exist.");
			createTable(tableName, t);
		} else {
			Log.info(Module.BICS,  "It exists. Nothing to do.");
		}
	}

	private static boolean tableExists(String tableName) {
		DefaultResponse dr = tableApi.getTableInfo(tableName.toUpperCase());

		if (dr.getContent().equals("[BICS-DATALOAD] There is no table by the name " + tableName))
			return false;

		return true;
	}

	private static void createTable(String tableName, Table t) {
		Log.info(Module.BICS, "Creating table '" + tableName + "'...");
		DefaultResponse dr = tableApi.createTable(tableName, t);
		Log.info(Module.BICS, dr.getContent() + " (Status Code: " + dr.getCode() + ")");
		if (dr.getCode() != 200) {
			Log.error(Module.BICS, "It was not possible to create table");
			System.exit(1);
		}
	}
	
	private static TableData createTableData(Table t) {
		TableData td = new TableData(false, null, null, WriteMode.UPSERT, cfg.getRightNowDelimiter(), "yyyy-MM-dd'T'hh:mm:ss'Z'", 0);
		
		for (int i = 0; i < t.count(); i++) {
			td.getDataloadColumnMaps().add(new DataloadColumnMap(new TableDataColumn(t.get(i).getColumnName(), i == 0 ? true: false), i + 1));
		}
		
		return td;
		
	}
}
