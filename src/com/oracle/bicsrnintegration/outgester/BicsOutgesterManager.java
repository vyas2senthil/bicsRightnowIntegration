package com.oracle.bicsrnintegration.outgester;

import java.util.ArrayList;

import com.oracle.bicsrnintegration.objects.BicsTableDescription;
import com.oracle.bicsrnintegration.utils.Log;
import com.oracle.bicsrnintegration.utils.Module;

public class BicsOutgesterManager implements Runnable {
	private static final int CHUNK_COUNT = 20;
	private static final int MAX_WORKERS = 4;

	private BicsOutgesterWorker[] workers;
	private ArrayList<String> queue;
	private boolean done;

	public BicsOutgesterManager(BicsTableDescription bicsTableDescription) {
		workers = new BicsOutgesterWorker[MAX_WORKERS];
		for (int i = 0; i < MAX_WORKERS; i++) {
			workers[i] = new BicsOutgesterWorker(i + 1, bicsTableDescription);
		}
		queue = new ArrayList<String>();
		done = false;
	}

	public synchronized void addChunk(String data) {
		queue.add(data);
	}

	private synchronized String getChunk() {
		if (queue.size() > 0)
			return queue.remove(0);

		return null;
	}

	private synchronized int getQueueSize() {
		return queue.size();
	}

	public void setDone() {
		done = true;
	}

	private BicsOutgesterWorker getAvailableWorker() {
		for (int i = 0; i < MAX_WORKERS; i++) {
			if (workers[i].lock()) {
				return workers[i];
			}
		}

		return null;
	}

	public void run() {
		Log.info(Module.BICSOUTGESTERMANAGER, "Started!");
		StringBuilder bigChunk = new StringBuilder();
		int chunkCount = 0;

		while (!done || getQueueSize() > 0) {
			while (chunkCount < CHUNK_COUNT && (!done || getQueueSize() > 0)) {
				String chunk;
				while ((chunk = getChunk()) == null) {
					//Log.info(Module.BICSOUTGESTERMANAGER, "Waiting for chunk...");
					try {
						if (done && getQueueSize() == 0) {
							Log.info(Module.BICSOUTGESTERMANAGER, "Finished!");
							return;
						}
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
						System.exit(1);
					}
				}
				bigChunk.append(chunk);
				chunkCount++;
			}

			BicsOutgesterWorker worker;
			while ((worker = getAvailableWorker()) == null) {
				//Log.info(Module.BICSOUTGESTERMANAGER, "Waiting for worker...");
				try {
					if (done && getQueueSize() == 0 && bigChunk.toString().equals("")) {
						Log.info(Module.BICSOUTGESTERMANAGER, "Finished!");
						return;
					}
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
					System.exit(1);
				}
			}
			
			Log.info(Module.BICSOUTGESTERMANAGER, "Found Worker #" + worker.getId() + " available!");

			worker.setChunk(bigChunk.toString());
			new Thread(worker).start();;
			Log.info(Module.BICSOUTGESTERMANAGER, "Worker #" + worker.getId() + " started!");
			
			bigChunk = new StringBuilder();
			chunkCount = 0;
		}
		
		Log.info(Module.BICSOUTGESTERMANAGER, "Finished!");
	}
}
