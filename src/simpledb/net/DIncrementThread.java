//
//  IncrementThread.java
//  
//
//  Created by Edmond Lau on 1/24/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.net;

import simpledb.*;
import simpledb.requests.*;

import java.io.*;
import java.net.*;
import java.util.*;

public class DIncrementThread extends CoordinatorThread {

	public static final int RUNS_PER_ABORT = 0;
	
	private int tuplePort;
	
	private String filename;	
	private int nRuns;
	private int nInsertsAtOnce;
	private int oneUpdateEveryN;
	private int nInsertsSinceLastUpdate;
	
	private int curRun;
	private int curVal;
	private TupleDesc td;
	private boolean aborted;
	
	private Random random;
	
	private static final int INITIAL_VAL = 16465920;// 1638400;//1638400 + 327680;
	
	private int curSegTupleId;
	
	// use this to make sure transaction ids don't overlap
	private int myTid;
	private static int baseTid = 0;
	private static final int MAX_TRANSACTIONS_PER_THREAD = 1000000;
	
	public DIncrementThread(String filename, int tuplePort, int nRuns, int nInsertsAtOnce, int oneUpdateEveryN) {
		this.tuplePort = tuplePort;
		this.nRuns = nRuns;
		this.filename = filename;
		this.td = Utilities.createTupleDesc(Utilities.columns(filename));
		this.nInsertsAtOnce = nInsertsAtOnce;
		curVal = INITIAL_VAL;
		
		this.oneUpdateEveryN = oneUpdateEveryN;
		nInsertsSinceLastUpdate = 0;
		
		random = new Random();
		curSegTupleId = 0;
		
		myTid = baseTid;
		baseTid += MAX_TRANSACTIONS_PER_THREAD;
		
		if (RUNS_PER_ABORT != 0)
			System.out.println("nRunsPerAbort: " + RUNS_PER_ABORT);
	}
	
	private int getRandomTupleId() {
		return random.nextInt(curVal);
	}
	
	private int getTupleIdFromNextSegment() {
		int ret = curSegTupleId;
		curSegTupleId += SegmentedHeapFile.SEGMENT_SIZE * 64;
		return ret;
	}
	

	private Tuple createTuple(int val) {
		Tuple tup = new Tuple(td);
		IntField intf = IntField.createIntField(val);
		for (int i = 2; i < td.numFields(); i++) {
			tup.setField(i, intf);
		}
		return tup;
	}

	public void run() {
		for (curRun = 1; curRun <= nRuns; curRun++) {
			if ((curRun % 1000 == 0) && (Debug.DEBUG_LEVEL == 0))
				System.out.print(".");
			aborted = false;
			runOnce();
			curVal+=nInsertsAtOnce;
			nInsertsSinceLastUpdate += nInsertsAtOnce;
			if (nInsertsSinceLastUpdate >= oneUpdateEveryN) {
				nInsertsSinceLastUpdate -= oneUpdateEveryN;
			}
		}
		
		println("Closing connections");
		closeConnections();		
	}

	public void runOnce() {
		int attempt = 0;
		
		while (true) {
			try {
				// begin transaction
				TransactionId tid = new TransactionId(myTid++);
			
				List<InetSocketAddress> catalogWorkers = SiteCatalog.Instance().getSitesFor(filename);
				Debug.println("workers: " + catalogWorkers);
				
//				println("Setting worker configuration to " + workers);
//				setWorkerConfiguration(workers);
//			
//				// connect to the sites
//				println("Initiating connections");
//				initiateConnections();
				if (!isSameWorkerConfiguration(catalogWorkers)) {
					System.out.println("Worker configuration changed");
					System.out.println("Old View: " + workers);
					System.out.println("Current View from Site Catalog: " + catalogWorkers);
					
					println("Closing connections");
					closeConnections();

					println("Setting worker configuration to " + catalogWorkers);
					setWorkerConfiguration(catalogWorkers);
				
					// connect to the sites
					println("Initiating connections");
					initiateConnections();			
				}

				// begin transaction
				println("Beginning transaction ");
				beginTransaction(tid);
				
				println("Spawning inserts/updates");
				for (int i = 0; i < nInsertsAtOnce; i++) {
					Request[] reqs = new Request[3];
					reqs[0] = new OpenFileReq(filename);
					// if this is the oneUpdateEveryN'th operation
					if (nInsertsSinceLastUpdate + i + 1 == oneUpdateEveryN) {
						Debug.println("Updating random tuple");
//						reqs[1] = new SpawnVersionedUpdateReq(0, getRandomTupleId(), createTuple(curVal+i));
						reqs[1] = new SpawnVersionedUpdateReq(0, getTupleIdFromNextSegment(), createTuple(curVal+i));
					} else {
						Debug.println("Inserting tuple");
						reqs[1] = new SpawnVersionedInsertReq(0, createTuple(curVal+i));
					}
					reqs[2] = new ExecuteReq(1);
					BatchReq batch = new BatchReq(reqs);
					sendReqToAll(batch);
				}
				
				if (curRun == nRuns) {
					Request req = new FlushReq();
					println("Sending flush req");
					sendReqToAll(req);
				}
				
//				try {
//					sleep(200);
//				} catch (InterruptedException ie) {
//					ie.printStackTrace();
//				}		
				
				if (RUNS_PER_ABORT != 0 && !aborted && (curRun % RUNS_PER_ABORT == 0)) {
					println("aborting");
					aborted = true;
					throw new TransactionAbortedException();
				} else {
					println("committing");
					distributedCommit();
					println("Transaction committed");
				}
				
//				println("Closing connections");
//				closeConnections();
					
				break;
			} catch (TransactionAbortedException e) {
//				try {
					println("Transaction aborted...");
					try {
						abort();
					} catch (IOException ioe) {
						println("IOException: " + ioe.getMessage());
					}
//					closeConnections();

//					sleep(random.nextInt((int)(Math.pow(2, attempt) * 100)));
//					if (attempt < 6)
//						attempt++;
//				} catch (InterruptedException ie) {
//					ie.printStackTrace();
//				}
			} catch (IOException ioe) {
				println("IOException: " + ioe.getMessage());
				println("Transaction aborted due to IOException...");
				try {
					abort();
				} catch (IOException e) {
					println("IOException: " + e.getMessage());
				}
				break;
			} catch (Exception e) {
				println("Unplanned exception: ");
				e.printStackTrace();
				break;
			}
		}

	}

}
