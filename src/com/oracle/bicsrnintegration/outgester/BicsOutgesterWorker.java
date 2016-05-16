package com.oracle.bicsrnintegration.outgester;

import com.oracle.bicsrnintegration.objects.BicsTableDescription;
import com.oracle.bicsrnintegration.utils.Log;
import com.oracle.bicsrnintegration.utils.Module;
import com.oracle.http.DefaultResponse;

public class BicsOutgesterWorker implements Runnable {

	private int id;
	private BicsTableDescription bicsTableDescription;
	private boolean available;
	private String chunk;

	public BicsOutgesterWorker(int id, BicsTableDescription bicsTableDescription) {
		this.id = id;
		this.bicsTableDescription = bicsTableDescription;
		chunk = null;
		available = true;
	}

	public boolean lock() {
		if (available) {
			available = false;
			return true;
		}
		return false;
	}

	public void setChunk(String chunk) {
		this.chunk = chunk;
	}
	
	public int getId() {
		return id;
	}

	public void run() {
		if (chunk == null) return;

		DefaultResponse dr;
		while (true) {
			Log.info(Module.BICSOUTGESTERWORKER, "[Worker #" + id + "]: " + "Inserting data into BICS table...");
			dr = bicsTableDescription.getTableDataApi().updateData(bicsTableDescription.getTableName(), bicsTableDescription.getTableData(), chunk);
			Log.info(Module.BICSOUTGESTERWORKER, "[Worker #" + id + "]: " + dr.getContent() + " (Status Code: " + dr.getCode() + ")");

			if (dr.getCode() == 200) break;
			
			if (dr.getCode() == 403) {
				Log.warning(Module.BICSOUTGESTERWORKER, "[Worker #" + id + "]: " + "Maximum number of API invocations exceeded. Trying again in a while...");
				continue;
			}
			
			Log.error(Module.BICSOUTGESTERWORKER, "[Worker #" + id + "]: " + "Unknown error.");
			System.exit(1);
		}

		available = true;

	}


}
