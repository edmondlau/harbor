//
//  TransactionTable.java
//  
//
//  Created by Edmond Lau on 2/6/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.log;

import simpledb.*;
import java.util.*;

/**
 * The TransactionTable stores information regarding the state of a transaction
 * for the purpose of ARIES recovery.
 *
 * For each transaction, we store its TransactionId, last lsn, 
 * state (prepared/unprepared), and undo next lsn.
 */
public class TransactionTable {

	private static final TransactionTable instance = new TransactionTable();

	private Map<TransactionId, TransactionRecord> table;

	public enum State { PREPARED, UNPREPARED };
	public static final State[] StateValues = State.values();

	public TransactionTable() {
		table = new HashMap<TransactionId, TransactionRecord>();
	}
	
	public static TransactionTable Instance() {
		return instance;
	}
	
	// clears the table
	public synchronized void clear() {
		table.clear();
	}
	
	public synchronized boolean containsKey(TransactionId tid) {
		return table.containsKey(tid);
	}
	
	public synchronized TransactionRecord get(TransactionId tid) {
		return table.get(tid);
	}

	public synchronized void insert(TransactionId tid, long lastLsn, State state, long undoNxtLsn) {
		TransactionRecord rec = new TransactionRecord(tid, lastLsn, state, undoNxtLsn);
		table.put(tid, rec);
	}
	
	public synchronized void delete(TransactionId tid) {
		table.remove(tid);
	}
	
	// @returns the set of records in the table
	public synchronized Set<TransactionRecord> records() {
		return new HashSet(table.values());
	}
	
	public synchronized void update(TransactionId tid, long lastLsn, State state, long undoNxtLsn) {
		if (containsKey(tid)) {
			TransactionRecord rec = get(tid);
			rec.lastLsn = lastLsn;
			rec.state = state;
			rec.undoNxtLsn = undoNxtLsn;
		} else {
			insert(tid, lastLsn, state, undoNxtLsn);
		}
	}
	
	public String toString() {
		StringBuffer buf = new StringBuffer();
		for (TransactionRecord tr : table.values()) {
			buf.append(tr.toString() + "\n");
		}
		return buf.toString();
	}

	class TransactionRecord {
		public TransactionId tid;
		public State state;
		public long lastLsn;
		public long undoNxtLsn;
	
		public TransactionRecord(TransactionId tid, long lastLsn, State state, long undoNxtLsn) {
			this.tid = tid;
			this.state = state;
			this.lastLsn = lastLsn;
			this.undoNxtLsn = undoNxtLsn;
		}
		
		public String toString() {
			return tid + " " + state + " " + lastLsn + " " + undoNxtLsn;
		}
	}

}
