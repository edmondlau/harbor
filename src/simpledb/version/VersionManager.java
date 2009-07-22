//
//  VersionManager.java
//  
//
//  Created by Edmond Lau on 1/30/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.version;

import simpledb.*;

import java.util.*;
import java.io.*;

/**
 * The VersionManager is a layer on top of the BufferPool that provides additional
 * support for a versioned row store capable of historical queries.
 */
public class VersionManager {

	public static final int INSERTION_COL = 0;
	public static final int DELETION_COL = 1;
	public static final int TUPLE_ID_COL = 2;
	
	public static final int INVISIBLE_TIME = Integer.MAX_VALUE;

	private static VersionManager instance;

	private Map<TransactionId, List<Tuple>> tidToInserts;
	private Map<TransactionId, List<RecordId>> tidToDeletes;
	
	private int chkptEpoch;

	private VersionManager() {
		tidToInserts = Collections.synchronizedMap(new HashMap<TransactionId, List<Tuple>>());
		tidToDeletes = Collections.synchronizedMap(new HashMap<TransactionId, List<RecordId>>());
		chkptEpoch = (int)Checkpoint.VERSIONED_CHKPT.read();
	}
	
	public static VersionManager Instance() {
		if (instance == null) {
			instance = new VersionManager();
		}
		return instance;
	}
	
	public void deleteTuple(TransactionId tid, Tuple tuple) 
		throws DbException, TransactionAbortedException {
		// acquire a lock, if not currently held
		RecordId rid = tuple.getRecordId();
		PageId pid = new PageId(rid.tableid(), rid.pageno());
		BufferPool.Instance().getPage(tid, pid, Permissions.READ_WRITE);
		// mark the tuple to be deleted
		updateMapping(tidToDeletes, tid, rid);
	}
	
	public RecordId insertTuple(TransactionId tid, int tableid, Tuple tuple) 
		throws DbException, IOException, TransactionAbortedException {
		// acquires a lock, if not currently held, and inserts a new tuple
		// set the insertion time uber high
		Tuple newTuple = new Tuple(tuple);
		newTuple.setField(INSERTION_COL, IntField.createIntField(INVISIBLE_TIME));		
		RecordId rid = BufferPool.Instance().insertTuple(tid, tableid, newTuple);
		newTuple.setRecordId(rid);
		updateMapping(tidToInserts, tid, newTuple);
		return rid;
	}
	
	public synchronized void updateTuple(TransactionId tid, Tuple tuple, UpdateFunction func) 
		throws DbException, TransactionAbortedException, IOException {
		Tuple newTuple = func.update(tuple);
		deleteTuple(tid, tuple);
		RecordId rid = insertTuple(tid, tuple.getRecordId().tableid(), newTuple);
	}
	
	private void updateMapping(Map map, TransactionId tid, Object obj) {
		List list;
		if (map.containsKey(tid)) {
			list = (List)map.get(tid);
		} else {
			list = new LinkedList();
			map.put(tid, list);
		}
		list.add(obj);
	}	
	
	public String toString() {
		return "Inserts: " + tidToInserts + "\nDeletes: " + tidToDeletes;
	}
	
	public void prepare(TransactionId tid) {
		BufferPool.Instance().prepareTransaction(tid);
	}
	
	// loops through and uses BufferPool.insertTuple and BufferPool.updateTuple
	// modify epoch stamps
	public void commit(final TransactionId tid, final int epoch) {
		if (epoch > chkptEpoch)
			chkptEpoch = epoch;
//		System.out.println(this);
		try {
			if (tidToInserts.containsKey(tid)) {
				List<Tuple> inserts = tidToInserts.get(tid);
				
				UpdateFunction insertFunction = 
					new UpdateFunction() {
							public Tuple update(Tuple t) {
								Tuple tuple = new Tuple(t);
								tuple.setField(INSERTION_COL, IntField.createIntField(epoch));
								return tuple;
							}
						};
				
				for (Tuple tuple : inserts) {
					RecordId rid = tuple.getRecordId();
					BufferPool.Instance().updateTuple(tid, rid, insertFunction);
				}
				tidToInserts.remove(tid);
			}
			if (tidToDeletes.containsKey(tid)) {
				List<RecordId> deletes = tidToDeletes.get(tid);

				UpdateFunction deleteFunction = 
					new DeleteFunction() {
							public Epoch getDeletionEpoch() {
								return new Epoch(epoch);
							}
							public Tuple update(Tuple t) {
								Tuple tuple = new Tuple(t);
								tuple.setField(DELETION_COL, IntField.createIntField(epoch));
								return tuple;
							}
						};

				for (final RecordId rid : deletes) {
					BufferPool.Instance().updateTuple(tid, rid, deleteFunction);
				}
				tidToDeletes.remove(tid);
			}
		} catch (Exception e) {
			System.out.println("should not happen");
			e.printStackTrace();
			System.exit(-1);
		}

		BufferPool.Instance().commitTransaction(tid);
	}
	
	public void abort(TransactionId tid) {
		// need to undo each insertion if logging is off, i.e. delete each inserted tuple
		if (tidToInserts.containsKey(tid)) {
			if (!BufferPool.Instance().loggingOn()) {
				try {
					for (Tuple tuple : tidToInserts.get(tid)) {
						BufferPool.Instance().deleteTuple(tid, tuple);
					}
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(-1);
				}
			}
			tidToInserts.remove(tid);
		}
		if (tidToDeletes.containsKey(tid)) {
			tidToDeletes.remove(tid);
		}
		BufferPool.Instance().abortTransaction(tid);
	}

	public void writeCheckpoint() {
		// chkptEpoch refers to last commit time we've seen
		int checkpointEpoch = chkptEpoch - 1;
		BufferPool.Instance().writeCheckpoint();
		Checkpoint.VERSIONED_CHKPT.write(checkpointEpoch);
	}
}
