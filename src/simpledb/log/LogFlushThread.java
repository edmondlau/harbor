//
//  LogFlushThread.java
//  
//
//  Created by Edmond Lau on 3/10/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.log;

import java.util.*;
import java.net.*;

/**
 * The LogFlushThread provides support for group commits by batching
 * a bunch of log records to disk.
 */
public class LogFlushThread extends Thread {

	int totalFlushEvents;
	int totalFlushes;
//	public static final int THRESHOLD = 90000;

	public static final int DELAY = 0;

	private Set<Object> pendingFlushes;
	private Set<Object> completedFlushes;

	public LogFlushThread() {
		setDaemon(true);
		pendingFlushes = new HashSet<Object>();
		completedFlushes = new HashSet<Object>();
		totalFlushEvents = 0;
		totalFlushes = 0;
	}
	
	private void printAverageBatch() {
		try {
			InetAddress me = InetAddress.getLocalHost();
			double avgBatch = ((double)totalFlushes) / totalFlushEvents;
			System.out.println("*** " + me + ": Average batch = " + avgBatch + " ***");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public synchronized void requestFlush() {
		Object request = new Object();
//		flushStatus.put(request, false);
//		notifyAll();
//		while (!flushStatus.get(request)) {
//			try {
//				wait();
//			} catch (InterruptedException e) {}
//		}
//		flushStatus.remove(request);
		pendingFlushes.add(request);
		notifyAll();
		while (!completedFlushes.contains(request)) {
			try {
				wait();
			} catch (InterruptedException e) {}
		}
		completedFlushes.remove(request);
	}
	
	public void run() {
		Set<Object> snapshot = new HashSet<Object>();	
		while (true) {
			synchronized (this) {
				while (pendingFlushes.size() == 0) {
					try {
						wait();
					} catch (InterruptedException e) {}
				}
			}
			
			// delay timer
			if (DELAY != 0) {
				try {
					sleep(DELAY);
				} catch (InterruptedException ie) {}
			}
			
			synchronized (this) {
//				totalFlushEvents++;
//				int prevTotal = totalFlushes;
//				totalFlushes += pendingFlushes.size();
//				if (prevTotal < THRESHOLD && totalFlushes >= THRESHOLD)
//					printAverageBatch();
				snapshot.addAll(pendingFlushes);
			}
			
			Log.Instance().flushLog();
			
			synchronized (this) {
				completedFlushes.addAll(snapshot);			
				pendingFlushes.removeAll(snapshot);
				notifyAll();
			}
			snapshot.clear();
			yield();
		}
	}
}