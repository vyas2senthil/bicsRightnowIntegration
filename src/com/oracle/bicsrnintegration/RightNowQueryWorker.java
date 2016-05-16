package com.oracle.bicsrnintegration;

import java.util.ArrayList;

import com.oracle.bicsrnintegration.outgester.BicsOutgesterManager;
import com.oracle.bicsrnintegration.utils.Log;
import com.oracle.bicsrnintegration.utils.Module;
import com.oracle.rightnow.RightNowClient;
import com.oracle.rightnow.RightNowResponse;

public class RightNowQueryWorker implements Runnable {
	
	private static final double LIMIT_INCREASE_RATE = 1.2;
	private static final double LIMIT_DECREASE_RATE = 0.5;
	private int threadId;
	private int minId;
	private int maxId;
	private String maxUpdatedTime;
	private String minUpdatedTime;
	private String queryAttributes;
	private String fromTable;
	private RightNowClient rn;
	private BicsOutgesterManager bicsOutgesterManager;
	
	public RightNowQueryWorker(int threadId, int minId, int maxId, String minUpdatedTime, String maxUpdatedTime,
			ArrayList<String> queryAttributes, String table, RightNowClient rn, BicsOutgesterManager bicsOutgesterManager) {
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
		this.fromTable = table;
		this.rn = rn;
		this.bicsOutgesterManager = bicsOutgesterManager;
	}
	
	public void run() {
		Log.info(Module.RIGHTNOW, "Hello! I am thread #" + threadId + " and I am responsible to query records from " + minId + " to " + maxId);
		int limit = maxId - minId + 1;
		int lastId = minId - 1;
		
		OptimalLimit optimalLimit = new OptimalLimit();
		while (lastId < maxId) {
			Log.info(Module.RIGHTNOW, "[Worker #" + threadId + "]: " + "Querying (from: " + (lastId + 1) + ", to: " + (lastId + limit) + ", limit: " + limit + ")...");
			String query = getQuery(lastId, limit);	
			
			RightNowResponse rightNowResponse = rn.execQuery(query, limit);
			if (rightNowResponse.isValid()) {
				Log.info(Module.RIGHTNOW, "[Worker #" + threadId + "]: " + "Request was successful and response was received!");

				String data = rightNowResponse.getResponse();
				bicsOutgesterManager.addChunk(data.substring(data.indexOf("\n") + 1));
				lastId = lastId + limit;
				optimalLimit.add(limit);
				limit = (int) (optimalLimit.get() * LIMIT_INCREASE_RATE);
			} else {
				Log.warning(Module.RIGHTNOW, "[Worker #" + threadId + "]: " + "Request was not successful due to: '" + rightNowResponse.getResponse() + "'");
				if (rightNowResponse.getResponse().equals("Poor performing query - too many rows examined")) {
					Log.warning(Module.RIGHTNOW, "[Worker #" + threadId + "]: " + "No problem! This error is treatable. Taking counter measures...");
					limit = (int) (limit * LIMIT_DECREASE_RATE);
					continue;
				} else {
					Log.error(Module.RIGHTNOW, "[Worker #" + threadId + "]: " + "Sorry, I don't know how to treat this problem.");
					System.exit(1);
				}
			}
		}
		
		Log.info(Module.RIGHTNOW, "[Worker #" + threadId + "]: " + "All records for this session were fetched!");
	}
	
	private String getQuery(int lastId, int limit) {
		return "SELECT " + queryAttributes + " FROM " + fromTable + 
				" WHERE ID &gt; " + lastId + " AND ID &lt; " + (lastId + limit + 1) + 
				" AND updatedtime &gt; '" + minUpdatedTime + "' AND updatedtime &lt; '" + maxUpdatedTime + "'";
	}
}
