package com.oracle.bicsrnintegration;

import java.util.Calendar;

import com.oracle.bics.BicsConnection;
import com.oracle.bics.apis.table.TableApi;
import com.oracle.bics.apis.table.objects.Table;
import com.oracle.http.DefaultResponse;
import com.oracle.rightnow.RightNowClient;
import com.oracle.rightnow.RightNowResponse;

public class Main {

	private static TableApi tableApi;

	private static String tableName = "RIGHTNOW_INCIDENTS";

	private static String rightNowTableName = "Incident";
	

	public static void main(String[] args) {
		Cfg cfg = Cfg.fromPropertiesFile();

		RightNowClient rn = new RightNowClient(cfg.getRightNowUrl(), cfg.getRightNowUser(), cfg.getRightNowPassword(), 
				cfg.getRightNowDelimiter(), cfg.getProxyServer(), cfg.getProxyPort());

		BicsConnection bics = new BicsConnection(cfg.getBicsService(), cfg.getBicsIdentityDomain(), cfg.getBicsDatacenter(),
				cfg.getBicsUser(), cfg.getBicsPassword(), cfg.getProxyServer(), cfg.getProxyPort());

		tableApi = new TableApi(bics);

		tableSetup();

		Calendar past = Calendar.getInstance(); past.set(1970, 1, 1, 0, 0, 1);
		String minUpdatedTime = RightNowClient.getTimestamp(past);

		Calendar now = Calendar.getInstance(); now.add(Calendar.HOUR, cfg.getHourDifference());
		String maxUpdatedTime = RightNowClient.getTimestamp(now);

		Log.log(Module.RIGHTNOW, "Getting MIN(ID) and MAX(ID) from '" + rightNowTableName + "'...");
		String[] minMax = rn.execQuery("USE REPORT; SELECT MIN(ID), MAX(ID) FROM " + rightNowTableName + " WHERE updatedtime &gt; '" + 
				minUpdatedTime + "' AND updatedtime &lt; '" + maxUpdatedTime + "'", 1).getResponse()
				.split("\\r\\n")[1].split(cfg.getRightNowDelimiter());
		Log.log(Module.RIGHTNOW,  "MIN(ID) = " + minMax[0] + " and MAX(ID) = " + minMax[1]);

		int minId = Integer.valueOf(minMax[0]), maxId = Integer.valueOf(minMax[1]);
		int limit = maxId - minId + 1;
		int lastId = minId - 1;
		
		OptimalLimit optimalLimit = new OptimalLimit();
		while (lastId < maxId) {
			Log.log(Module.RIGHTNOW, "Querying (from: " + lastId + ", to: " + (lastId + limit + 1) + ", limit: " + limit + ")...");
			String query = "SELECT " + getRightNowQueryAttributes() + " FROM " + rightNowTableName + 
					" WHERE ID &gt; " + lastId + " AND ID &lt; " + (lastId + limit + 1) + 
					" AND updatedtime &gt; '" + minUpdatedTime + "' AND updatedtime &lt; '" + maxUpdatedTime + "'";		
			RightNowResponse rightNowResponse = rn.execQuery(query, limit);
			if (rightNowResponse.isValid()) {
				Log.log(Module.RIGHTNOW, "Request was succesful! " + (rightNowResponse.getResponse().split("\\r\\n").length - 1) + 
						" record(s) received!");
				lastId = lastId + limit;
				optimalLimit.add(limit);
				limit = (int) (optimalLimit.get() * 1.2);
				
			} else {
				Log.log(Module.RIGHTNOW, "Request was not succesful due to: '" + rightNowResponse.getResponse() + "'");
				if (rightNowResponse.getResponse().equals("Poor performing query - too many rows examined")) {
					Log.log(Module.RIGHTNOW, "No problem! This error is treatable. Taking counter measures...");
					limit = limit / 2;
					continue;
				} else {
					Log.log(Module.RIGHTNOW, "Sorry, I don't know how to treat this problem. Shutting down...");
					System.exit(1);
				}
			}
		}
		
		Log.log(Module.RIGHTNOW, "All records for this session were fetched!");

	}

	public static void tableSetup() {
		Log.log(Module.BICS, "Checking if table '" + tableName + "' exists...");
		if (!tableExists(tableName)) {
			Log.log(Module.BICS,  "It does not exist.");
			createTable(tableName, null); // TODO: get Table from file
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

	public static String getRightNowQueryAttributes() {
		return "ID, "
				+ "Incident.StatusWithType.Status.Name , "
				+ "Incident.AssignedTo.Account.ID , "
				+ "Incident.Category.LookupName , "
				+ "Incident.Category.Level1.LookupName , "
				+ "Incident.Category.Level2.LookupName , "
				+ "Incident.Category.Level3.LookupName , "
				+ "Incident.Category.Level4.LookupName , "
				+ "Incident.Disposition.Level1.LookupName , "
				+ "Incident.Disposition.Level2.LookupName , "
				+ "ReferenceNumber , "
				+ "CreatedTime , "
				+ "ClosedTime ";
	}


}
