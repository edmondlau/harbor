//
//  RecoveryThread.java
//  
//
//  Created by Edmond Lau on 1/24/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.net;

import simpledb.*;
import simpledb.requests.*;
import simpledb.version.*;

import java.io.*;
import java.net.*;
import java.util.*;

// Todo:
// Non-locking mode
// Exclusive lock request on site, table
// 

// Recovery protocol
// - add recovery budd(y/ies)
// - initiate connection
// - begin snapshot isolated transaction
//		- seq scan, time <= HWM
//		- insert tuples locally
//		- commit snapshot isolated transaction
// - initiate new connection -- is it possible to skip this step? we'd need an END connection then
// - begin new transaction
//		- seq scan, HWM < time <= current time
// - no pending transactions? done

public class RecoveryThread extends CoordinatorThread {

	private static int THREAD_COUNT = 0;

	public static final int USE_REAL_HWM = -1;

	private InetAddress me;

	private InetSocketAddress buddy;
	
	private InetSocketAddress coordinator;
	
	private static final int TUPLE_BASE_PORT_BASE = 14000;
	
	private static final int RECOVERY_WORKER_PORT_BASE = 9001;
	
	private String filename;
	private DbFile file;	
	private int tupleSize;
	
	private TransactionId lockId;
	
	private Random random;
	private int hwmValue;
	
	private IntField hwm;
	
	private IntField chkptField;
	private Epoch chkptEpoch;
	
	public static final int LOCK_ID_BASE =    666666666;
	public static final int PHASE_1_ID_BASE = 111111111;
	public static final int PHASE_2_ID_BASE = 222222222;
	public static final int PHASE_3_ID_BASE = 333333333;
	
	private int uniquifier;
	
	private int TUPLE_BASE_PORT;
	private int RECOVERY_WORKER_PORT;
	private int LOCK_ID;
	private int PHASE_1_ID;
	private int PHASE_2_ID;
	private int PHASE_3_ID;
	
	public RecoveryThread(String filename, int hwm, int uniquifier) {
		System.out.println("UNIQUIFIER: " + uniquifier);
		synchronized(RecoveryThread.class) {
			THREAD_COUNT++;
		}
		BufferPool.Instance().setLogging(false);
		random = new Random();
		this.filename = filename;
		tupleSize = Utilities.columns(filename);
		file = Catalog.Instance().getDbFile(filename);
		
		int checkpoint = (int)Checkpoint.VERSIONED_CHKPT.read();
		chkptField = IntField.createIntField(checkpoint);
		chkptEpoch = new Epoch(chkptField.val());
		
		hwmValue = hwm;
		
		this.uniquifier = uniquifier;
		
		TUPLE_BASE_PORT = TUPLE_BASE_PORT_BASE + uniquifier;
		RECOVERY_WORKER_PORT = RECOVERY_WORKER_PORT_BASE + uniquifier;
		LOCK_ID = LOCK_ID_BASE + uniquifier;
		PHASE_1_ID = PHASE_1_ID_BASE + uniquifier;
		PHASE_2_ID = PHASE_2_ID_BASE + uniquifier;		
		PHASE_3_ID = PHASE_3_ID_BASE + uniquifier;
				
		try {
			me = InetAddress.getLocalHost();
			buddy = SiteCatalog.Instance().getSitesFor(filename).get(0);
			coordinator = SiteCatalog.Instance().getCoordinators().get(0);
		} catch (UnknownHostException uhe) {
			uhe.printStackTrace();
			//System.exit(-1);
		}
	}
	
	/**
	 * Phase 1A: Delete all tuples with insertion times > checkpoint.
	 */
	private void deleteInsertionsAfterCheckpoint(TransactionId tid) 
			throws DbException, TransactionAbortedException {			
		int tableid = file.id();
//		DELETE LOCALLY FROM rec
//      SEE DELETED
//		WHERE insertion_epoch > chkpt_epoch
		
		IndexedSeqScan ss = new IndexedSeqScan(tid, tableid, true);
		ss.restrictInsertionTime(new Epoch(chkptEpoch.value()+1), new Epoch(Integer.MAX_VALUE));
		Predicate p = new Predicate(VersionManager.INSERTION_COL, Predicate.GREATER_THAN, chkptField);
		Filter f = new Filter(p, ss);
		Delete delOp = new Delete(tid, f);
		
		println("Deleting tuples from " + filename + ": " + p);

		int n = Utilities.execute(delOp);
		System.out.println("Deleted " + n + " tuples");
	}
	
	/**
	 * Phase 1B: Undelete all tuples with deletion times > checkpoint.
	 */	
	private void undoDeletionsAfterCheckpoint(TransactionId tid)
			throws DbException, TransactionAbortedException {
//		UPDATE LOCALLY rec SET deletion_epoch = null
//      SEE DELETED
//		WHERE deletion_epoch > chkpt_epoch (AND insertion_epoch <= chkpt_epoch) -- can be avoided if we delete empty segments
		int tableid = file.id();
		
		IndexedSeqScan ss = new IndexedSeqScan(tid, tableid, true);
		ss.restrictInsertionTime(new Epoch(0), chkptEpoch);
		ss.restrictDeletionTime(new Epoch(chkptEpoch.value() + 1));
		Predicate p = new Predicate(VersionManager.DELETION_COL, Predicate.GREATER_THAN, chkptField);
		Filter f = new Filter(p, ss);
		
		UpdateFunction func = new UpdateFunction() {
			public Tuple update(Tuple t) {
				Tuple tuple = new Tuple(t);
				tuple.setField(VersionManager.DELETION_COL, IntField.createIntField(0));
				return tuple;
			}
		};
		
		println("Undeleting tuples in " + filename + ": " + p);
		Update updateOp = new Update(tid, f, func);
		int n = Utilities.execute(updateOp);
		System.out.println("Undeleted " + n + " tuples");		
	}
	
	/**
	 * Phase 1A&B: Restores the SegmentedHeapFile specified by filename to 
	 * its state at the time of the last checkpoint.
	 */		
	private void restoreCheckpoint() 
			throws DbException, TransactionAbortedException {
		TransactionId tid = new TransactionId(PHASE_1_ID);
		
//		DELETE LOCALLY FROM rec
//      SEE DELETED
//		WHERE insertion_epoch > chkpt_epoch	
		ActionTimer.start(this);
		println("Deleting insertions after checkpoint");
		deleteInsertionsAfterCheckpoint(tid);
		System.out.println("Phase 1A Time for deletion: " + ActionTimer.stop(this));	

//		UPDATE LOCALLY rec SET deletion_epoch = null
//      SEE DELETED
//		WHERE deletion_epoch > chkpt_epoch
		ActionTimer.start(this);
		println("Undoing deletions after checkpoint");
		undoDeletionsAfterCheckpoint(tid);
		System.out.println("Phase 1B Time for update: " + ActionTimer.stop(this));		
		
		BufferPool.Instance().commitTransaction(tid);
	}

	
	/**
	 * Phase 2A and 3A: Send a request for deletion times.
	 */
	private void requestDeletionTimes(int tuplePort, SpawnIndexedSeqScanReq ssReq, Predicate pred)
			throws IOException {
		// hack: assume that the tuple id are in order and can be used to deduce the record id
		int cols[] = { VersionManager.TUPLE_ID_COL, VersionManager.DELETION_COL};					
		
		Request[] reqs = new Request[6];
		reqs[0] = new OpenFileReq(filename);
		ssReq.handle = 0;
		reqs[1] = ssReq;
		reqs[2] = new SpawnFilterReq(1, pred);
		reqs[3] = new SpawnProjectReq(2, cols);
		reqs[4] = new SpawnNetInsertReq(me.getHostName(), tuplePort, 3);
		reqs[5] = new ExecuteReq(4);
		BatchReq batch = new BatchReq(reqs);
		println("Sending batch request ");				
		sendReqToAll(batch);
	}
	
	/**
	 * Phase 2B and 3B: Update deletion times received over the network
	 */	
	private void updateDeletionTimes(int tuplePort) throws DbException, TransactionAbortedException {		
		int tableid = file.id();
		TupleDesc td = Catalog.Instance().getTupleDesc(tableid);
		TupleDesc pair = Utilities.createTupleDesc(2);
		
		// boot up local net scan and update operators
		println("Spawned local net scan");
		NetScan netScan = new NetScan(tid, pair, tuplePort);

		List tuples = new LinkedList<Tuple>();
		UpdateDeletionFunc func = new UpdateDeletionFunc();

		int n = 0;

		try {
			netScan.open();
			while (true) {
				Tuple tuple = netScan.getNext();
				func.deletionEpoch = tuple.getField(1);
				IntField tupleId = (IntField)tuple.getField(0);
				tuple.setRecordId(Utilities.tupleIdToRecordId(tupleId.val(), tableid, td));
				tuples.add(tuple);
				
				println("Spawned local update for " + tuple);
				Update update = new Update(tid, new TupleIterator(pair, tuples), func);
				
				println("Running local update");
				Utilities.execute(update);
				n++;
				
				tuples.clear();
			}
		} catch (NoSuchElementException e) {
			netScan.close();
		}
		
		System.out.println("Updated " + n + " tuples");
	}
	
	/**
	 * Phase 2A&B: Restores the all data before the checkpoint to how it looked like at hwm
	 */
	private void updateCheckpointToHWM(TransactionId tid, int tuplePort) 
		throws DbException, TransactionAbortedException, IOException {
//		(tup_id, del_epoch) = 
//		SELECT REMOTELY tuple_id, deletion_epoch FROM recovery_buddy_object
//		SEE DELETED
//		WHERE buddy_predicate
//			AND insertion_epoch <= chkpt_epoch
//			AND deletion_epoch > chkpt_epoch
		
		// need to do this remotely
		ConjunctivePredicate pred = 
			new ConjunctivePredicate(
				ConjunctivePredicate.AND,
				new Predicate(VersionManager.INSERTION_COL, Predicate.LESS_THAN_OR_EQ, chkptField),
				new Predicate(VersionManager.DELETION_COL, Predicate.GREATER_THAN, chkptField));		
					
		SpawnIndexedSeqScanReq ssReq = 
			new SpawnIndexedSeqScanReq(0, 0, chkptEpoch.value(), chkptEpoch.value()+1, hwm.val(), true);			
		
		// request that the deletion times be sent to tuplePort
		println("Requesting deletion times for " + filename + " to :" + tuplePort + " with " + ssReq + "," + pred);
		requestDeletionTimes(tuplePort, ssReq, pred);
		// update local tuples with deletion times received via tuplePort
		println("Updating local deletion times");
		updateDeletionTimes(tuplePort);
	}
	
	/**
	 * Phase 2C & 3C: Set up NetScan operator to listen for tuples
	 */
	private Insert prepareTupleListener(int tuplePort) throws DbException, TransactionAbortedException {
		int tableid = file.id();
		
		// boot up local seq scan and insert operators
		println("Spawned local net scan");
		NetScan netScan = new NetScan(tid, Catalog.Instance().getTupleDesc(tableid), tuplePort);
	
		println("Spawned local insert");
		Insert insert = new Insert(tid, netScan, tableid);
		return insert;		
	}
	
	/**
	 * Phase 2C & 3C: Query for newly inserted tuples
	 */
	private void query(int tuplePort, SpawnIndexedSeqScanReq ssReq, Predicate pred) throws IOException {
		Request[] reqs = new Request[5];
		reqs[0] = new OpenFileReq(filename);
		ssReq.handle = 0;
		reqs[1] = ssReq;
		reqs[2] = new SpawnFilterReq(1, pred);
		reqs[3] = new SpawnNetInsertReq(me.getHostName(), tuplePort, 2);
		reqs[4] = new ExecuteReq(3);
		BatchReq batch = new BatchReq(reqs);
		println("Sending batch request ");	
		sendReqToAll(batch);
	}
	
	/**
	 * Phase 2D & 3D: Insert newly received tuples.
	 */	
	 
	private void insertLocally(Insert insert) throws DbException, TransactionAbortedException {
		int n = Utilities.execute(insert);
		System.out.println("Inserted " + n + " tuples");
	}
	
//	private void insertLocally(int tuplePort) throws DbException, TransactionAbortedException {
//		int tableid = file.id();
//		
//		// boot up local seq scan and insert operators
//		println("Spawned local net scan");
//		NetScan netScan = new NetScan(tid, Catalog.Instance().getTupleDesc(tableid), tuplePort);
//		
//		println("Spawned local insert");
//		Insert insert = new Insert(tid, netScan, tableid);
//		
//		int n = Utilities.execute(insert);	
//		System.out.println("Inserted " + n + " tuples");
//	}
	
	/**
	 * Phase 2C&D: Query and insert new tuples b/w checkpoint and hwm.
	 */
	private void queryInsertionsBetweenCheckpointAndHWM(int tuplePort) throws IOException {
//		INSERT LOCALLY INTO rec
//			SELECT REMOTELY * FROM recovery_buddy_object
//			SEE DELETED
//			WHERE buddy_predicate
//				AND insertion_epoch > chkpt_epoch (AND insertion_epoch <= hwm)
		Predicate pred = new Predicate(VersionManager.INSERTION_COL, Predicate.GREATER_THAN, chkptField);
		SpawnIndexedSeqScanReq ssReq = 
			new SpawnIndexedSeqScanReq(0, chkptEpoch.value()+1, hwm.val(), IndexedSeqScan.NO_DELETE_RESTRICTION, hwm.val(), true);			
		query(tuplePort, ssReq, pred);
	}
	
	// Phase 2: Use historical queries to catch up to HWM
	private void catchupToHWM(int tuplePort) throws DbException, TransactionAbortedException, IOException {
		ActionTimer.start(this);
		TransactionId tid = new TransactionId(PHASE_2_ID);
		// begin the recovery transaction
		println("Beginning transaction to update checkpoint to hwm " + hwm);
		beginTransaction(tid); // true means no locks	
		
		// updates all data inserted before the checkpoint to hwm
		println("Updating pre-checkpoint data to hwm " + hwm);
		updateCheckpointToHWM(tid, tuplePort);
		System.out.println("Phase 2A&2B Time to update pre-checkpoint data to HWM: " + ActionTimer.stop(this));
		
		ActionTimer.start(this);
		// grabs all data inserted before checkpoint and hwm
		println("Querying for insertions b/w checkpoint " + chkptEpoch + " and " + hwm);
		Insert insert = prepareTupleListener(tuplePort+1);
		queryInsertionsBetweenCheckpointAndHWM(tuplePort+1);
		// insert the data locally
		println("Inserting locally");
		insertLocally(insert);
		System.out.println("Phase 2C&D Time to query for Insertions b/w Checkpoint and HWM: " + ActionTimer.stop(this));		
		
		println("One phase commit");
		commit();
	}
	
	private void queryInsertionsAfterHWM(int tuplePort) throws IOException {
//		INSERT LOCALLY INTO rec
//			SELECT REMOTELY * FROM recovery_buddy_object
//			SEE DELETED
//			SET LOCKS
//			WHERE buddy_predicate
//			AND insertion_epoch > hwm
		Predicate pred = new Predicate(VersionManager.INSERTION_COL, Predicate.GREATER_THAN, hwm);
		SpawnIndexedSeqScanReq ssReq = 
			new SpawnIndexedSeqScanReq(0, hwm.val()+1, Integer.MAX_VALUE, IndexedSeqScan.NO_DELETE_RESTRICTION, Integer.MAX_VALUE, true);
			// we cheat a little by using a really high hwm to read data w/o locks since we 
			// already have the table lock
		query(tuplePort, ssReq, pred);
	}
	
	// we want deletion times of data inserted before HWM but deleted after
	private void updateDeletionsAfterHWM(int tuplePort) throws DbException, TransactionAbortedException, IOException {
		// need to do this remotely
		ConjunctivePredicate pred = 
			new ConjunctivePredicate(
				ConjunctivePredicate.AND,
				new Predicate(VersionManager.INSERTION_COL, Predicate.LESS_THAN_OR_EQ, hwm),
				new Predicate(VersionManager.DELETION_COL, Predicate.GREATER_THAN, hwm));		
					
		SpawnIndexedSeqScanReq ssReq = new SpawnIndexedSeqScanReq(0, 0, hwm.val(), hwm.val()+1, Integer.MAX_VALUE, true);
			// we cheat a little by using a really high hwm to read data w/o locks since we 
			// already have the table lock
		
		// request that the deletion times be sent to tuplePort
		requestDeletionTimes(tuplePort, ssReq, pred);
		// update local tuples with deletion times received via tuplePort
		updateDeletionTimes(tuplePort);
	}
	
	private void catchupToCurrentTime(int tuplePort) throws DbException, TransactionAbortedException, IOException {
		ActionTimer.start(this);
		TransactionId tid = new TransactionId(PHASE_3_ID);
		println("Beginning transaction to catch up to current time ");
		beginTransaction(tid);	
	
//		ACQUIRE REMOTELY READ LOCK ON recovery_buddy_object	
		println("Acquiring lock on " + filename);
		acquireLock(filename);
		
		// get data inserted after hwm
		println("Querying for insertions " + filename);	
		Insert insert = prepareTupleListener(tuplePort+2);
		queryInsertionsAfterHWM(tuplePort+2);
		
		System.out.println("Phase 3A&3B Time to get inserts after HWM: " + ActionTimer.stop(this));
		
		ActionTimer.start(this);
		
		println("Inserting locally");
		insertLocally(insert);
		
		// get deletions after hwm
		println("Querying for deletions " + filename);				
		updateDeletionsAfterHWM(tuplePort+3);
		
		System.out.println("Phase 3C&D Time to query for post-hwm deletions to old data: " + ActionTimer.stop(this));		
		
		
		final int workerPort = RECOVERY_WORKER_PORT;
		startWorker(workerPort);
		
		try {
			Debug.println("Joining coordinator's pending transactions");
			joinPendingTransactions(workerPort);
		} catch (IOException ioe) {
			println("Failed to join coordinator's pending transactions");
			println("IOException @ RecoveryThread: " + ioe.getMessage());
		}

//		Debug.println("Sleeping");
//		try {
//			sleep(2000);
//		} catch (InterruptedException ie) {}		
		println("Releasing lock on " + filename);
		releaseLock();
		
		println("Committing");		
		commit();
	}
	
	private void recover() throws DbException, TransactionAbortedException, IOException {
		addWorker(buddy);	
		
		System.out.println("Phase 1 - Restoring checkpoint...");
		restoreCheckpoint();
		
		if (hwmValue == USE_REAL_HWM)
			hwm = IntField.createIntField(TimestampAuthority.Instance().getTime().value() - 1);
		else
			hwm = IntField.createIntField(hwmValue);
		
		System.out.println("Phase 2 - Catching up to HWM " + hwm + "...");

		println("Initiating connections");
		initiateConnections();
		catchupToHWM(TUPLE_BASE_PORT);
		
//		Checkpoint.VERSIONED_CHKPT.write(hwm.val());
		
		System.out.println("Phase 3 - Catching up to committed data after HWM...");
		catchupToCurrentTime(TUPLE_BASE_PORT);
//		System.out.println("Phase 3 - Time to catch up to committed data after HWM: " + ActionTimer.stop(this));				

		println("Closing connections");
		closeConnections();

		BufferPool.Instance().flush_all_pages();
	}
	
	private void startWorker(final int workerPort) {
		Debug.println("Starting worker communication hub at port " + workerPort);
		new Thread() {
			public void run() {
				ServerSocket server = null;
				try {
					server = Utilities.createServerSocket(workerPort);	
					// spawn worker threads to handle connections
					while (true) {
						Socket socket = Utilities.accept(server);
						Debug.println("Accepting connection");
						WorkerThread thread = new WorkerThread(socket);
		//				thread.run();
						thread.start();
					}
				} catch (IOException ioe) {
					System.err.println("CommHub could not listen on port: " + workerPort);
					System.exit(-1);
				} finally {
					try {
						if (server != null) {
							server.close();
						}
					} catch (IOException ioe) {}
				}
			}
		}.start();
	}
	
	private void joinPendingTransactions(final int workerPort) throws IOException {
		List<String> relations = new ArrayList<String>();
		relations.add(filename);
		JoinReq join = new JoinReq(InetAddress.getLocalHost().getHostName(), workerPort, relations);
		
		Socket socket = Utilities.createClientSocket(coordinator);
//		Socket socket = new Socket(coordinator.getHostName(), coordinator.getPort());
		CommLayer comm = new CommLayer(socket, false);
		comm.write(join);
		Request ack = comm.read();
		comm.close();
	}
	
	public void acquireLock(String filename) throws IOException {
		lockId = new TransactionId(LOCK_ID);
		AcquireTableLockReq req = new AcquireTableLockReq(lockId, filename);
		for (CommLayer comm : workerToComm.values()) {
			comm.write(req);
		}
		for (CommLayer comm : workerToComm.values()) {
			GrantLockAck ack = (GrantLockAck)comm.read();
		}
	}
	
	public void releaseLock() throws IOException {
		ReleaseTableLockReq req = new ReleaseTableLockReq(lockId);
		for (CommLayer comm : workerToComm.values()) {
			comm.write(req);
		}
	}
	
	public void run() {
		try {
			BufferPool.Instance().setLogging(false);
			recover();		
		} catch (Exception e) {
			e.printStackTrace();
			try {
				Debug.println("Releasing lock");
				releaseLock();
				
				println("Closing connections");
				closeConnections();				
			} catch (IOException ioe) {
			
			}
			System.exit(-1);
		}
		synchronized (RecoveryThread.class) {
			THREAD_COUNT--;
			if (THREAD_COUNT <= 0)
				System.out.println("Total Recovery Time: " + ActionTimer.stop(ActionTimer.RECOVERY));
		}
	}
	
	class UpdateDeletionFunc implements UpdateFunction {
		public Field deletionEpoch = null;
		
		public Tuple update(Tuple t) {
			Tuple tuple = new Tuple(t);
			tuple.setField(VersionManager.DELETION_COL, deletionEpoch);
			return tuple;
		}		
	}
}
