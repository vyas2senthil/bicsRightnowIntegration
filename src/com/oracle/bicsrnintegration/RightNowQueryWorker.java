package com.oracle.bicsrnintegration;

import java.util.ArrayList;

import com.oracle.rightnow.RightNowClient;
import com.oracle.rightnow.RightNowResponse;

public class RightNowQueryWorker implements Runnable {

	private int threadId;
	
	private int minId;
	private int maxId;
	
	private String maxUpdatedTime;
	private String minUpdatedTime;
	
	private String queryAttributes;
	private String table;
	
	private RightNowClient rn;
	
	public RightNowQueryWorker(int threadId, int minId, int maxId, String minUpdatedTime, String maxUpdatedTime,
			ArrayList<String> queryAttributes, String table, RightNowClient rn) {
		this.threadId = threadId;
		this.minId = minId;
		this.maxId = maxId;
		this.maxUpdatedTime = maxUpdatedTime;
		this.minUpdatedTime = minUpdatedTime;
		this.queryAttributes = "";
		for (String attr : queryAttributes) {
			this.queryAttributes += attr + ",";
		}
		this.queryAttributes = this.queryAttributes.substring(0, this.queryAttributes.length() - 1);
		this.table = table;
		this.rn = rn;
	}



	public void run() {
		Log.log(Module.RIGHTNOW, "Hello! I am thread #" + threadId + " and I am responsible to query records from " + minId + " to " + maxId);
		int limit = maxId - minId + 1;
		int lastId = minId - 1;
		
		OptimalLimit optimalLimit = new OptimalLimit();
		while (lastId < maxId) {
			Log.log(Module.RIGHTNOW, "Querying (from: " + (lastId + 1) + ", to: " + (lastId + limit) + ", limit: " + limit + ")...");
			String query = "SELECT " + queryAttributes + " FROM " + table + 
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

}
