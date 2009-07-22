//
//  DIncrementTest.java
//  
//
//  Created by Edmond Lau on 1/24/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.test;

import simpledb.*;
import simpledb.net.*;
import simpledb.requests.*;

import java.net.*;

public class DIncrementTest extends Test {

	public static int TUPLE_PORT = 8000;

	public boolean runTest(String args[]) {
		if (args.length != 7) {
			System.out.println("Usage: <-2pc|-3pc> filename nTrans nThreadsAtOnce nInsertsAtOnce oneUpdateEveryN");
			System.exit(-1);
		}

		if (args[1].equals("-2pc")) {
			BufferPool.Instance().setLogging(true);
			CoordinatorThread.setCommitProtocol(CoordinatorThread.CommitProtocol.TWO_PC);
		} else if (args[1].equals("-3pc")) {
			BufferPool.Instance().setLogging(false);
			CoordinatorThread.setCommitProtocol(CoordinatorThread.CommitProtocol.THREE_PC);			
		} else {
			System.out.println("Usage: <-2pc|-3pc> filename nTrans nThreadsAtOnce nInsertsAtOnce oneUpdateEveryN");
			System.exit(-1);		
		}
		String filename = args[2];
		int nTrans = Integer.parseInt(args[3]);
		int nThreadsAtOnce = Integer.parseInt(args[4]);
		int nInsertsAtOnce = Integer.parseInt(args[5]);
		int oneUpdateEveryN = Integer.parseInt(args[6]);
		System.out.println("Running IncrementTest on " + nTrans + " threads, " + nThreadsAtOnce + 
			" at a time, " + nInsertsAtOnce + " inserts at once, one update every " + oneUpdateEveryN);
		
		// boot up IncrementThreads to coordinate
		CoordinatorThread[] threads = new CoordinatorThread[nThreadsAtOnce];
	
		for (int i = 0; i < nThreadsAtOnce; i++) {
			String modifiedFilename = header(filename) + i + tail(filename);
			System.out.println("Spawning DIncrementThread for " + modifiedFilename);
			threads[i] = new DIncrementThread(modifiedFilename, TUPLE_PORT+i, nTrans/nThreadsAtOnce, nInsertsAtOnce, oneUpdateEveryN);
			threads[i].start();
		}
	  
		return true;
	}
	
	public String header(String filename) {
		return filename.substring(0, filename.indexOf(".") + 1);
	}
	
	public String tail(String filename) {
		return filename.substring(filename.indexOf("."));
	}
}
