//
//  CommHub.java
//  
//
//  Created by Edmond Lau on 1/23/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.net;

import java.net.*;
import java.io.*;
import java.util.*;

import simpledb.*;
import simpledb.requests.*;
import simpledb.test.*;

public class CommHub extends Test {
	// the port that coordinators should connect to
	public static final int DEFAULT_COMM_PORT = 9000;
	
	private int commPort;
	
	public enum TransactionState { UNINITIALIZED, PENDING, PENDING_EXEC, IN_DOUBT, COMMITTED, ABORTED, ENDED };
	
	private Map<TransactionId, TransactionState> tidToState;
	
	public CommHub(int commPort) {
		this.commPort = commPort;
		tidToState = new HashMap<TransactionId, TransactionState>();
	}
	
	public void acceptAndDispatchConnections() {
		ServerSocket server = null;
		try {
			server = Utilities.createServerSocket(commPort);	
			// spawn worker threads to handle connections
			while (true) {
				Socket socket = Utilities.accept(server);
				Debug.println("Accepting connection");
				WorkerThread thread = new WorkerThread(socket);
//				thread.run();
				thread.start();
			}
		} catch (IOException ioe) {
			System.err.println("CommHub could not listen on port: " + commPort);
			System.exit(-1);
		} finally {
			try {
				if (server != null) {
					server.close();
				}
			} catch (IOException ioe) {}
		}
	}
	
//	public synchronized void updateTransactionStatus(TransactionId tid, TransactionState state) {
//		tidToState.put(tid, state);
//	}
//	
//	public synchronized List<TransactionId> getPendingTransactions() {
//		List<TransactionId> tids = new LinkedList();
//		for (TransactionId tid : tidToState.keySet()) {
//			if (tidToState.get(tid) == TransactionState.PENDING) {
//				tids.add(tid);
//			}
//		}
//		return tids;
//	}
	
	public static void main(String[] args) {
		CommHub hub = new CommHub(DEFAULT_COMM_PORT);
		hub.acceptAndDispatchConnections();
	}
}