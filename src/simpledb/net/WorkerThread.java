//
//  WorkerThread.java
//  
//
//  Created by Edmond Lau on 1/23/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.net;

import java.io.*;
import java.net.Socket;
import java.util.*;

import simpledb.*;
import simpledb.requests.*;
import simpledb.version.*;
import simpledb.log.*;
import simpledb.net.CoordinatorHub.*;
import simpledb.net.CommHub.*;

public class WorkerThread extends Thread {

	// QUESTION: need to recycle connection if a transaction aborts? no - not good.  kill connection
	// if local abort, need to stay alive until prepare message is sent.

	// unique worker id on the worker machine
	private static int THREAD_ID = 0;
	
	private static int WORK_UNIT = 100000;
	
	private int threadId;
	
	// unique handle generator
	private static long HANDLE_COUNTER = 0;
	
	// socket variables
	private Socket socket;
	private CommLayer comm;
//	private ObjectOutputStream out;
//	private ObjectInputStream in;
	
	// transaction variables
	private TransactionId tid;
	// variable to notify execution thread that transaction execution is done
	private TransactionState transState;
	
	// types of aborts
	enum AbortType { COORDINATED, LOCAL_TIMEOUT, LOCAL_DB_ERROR, SOCKET_ERROR };
	enum AbortSrc { WORKER, EXECUTOR };
	
	// mapping from handles => db operators
	private Map<Long, DbIterator> handlesToOps;
	
	// list of handles to be run locally, in the specified order
	private List<Long> runHandles;
	
	// execution thread to run operators
	private ExecutionThread executor;
	
	private Random rand;
	
	private boolean flush;
	
	private static int work = 0;
	
	public WorkerThread(Socket socket) {
		super("Worker-" + THREAD_ID);
		threadId = THREAD_ID++;
		this.socket = socket;
		handlesToOps = new HashMap<Long, DbIterator>();
		runHandles = new LinkedList<Long>();
		transState = TransactionState.UNINITIALIZED;
		rand = new Random();
		
		comm = new CommLayer(socket, CommLayer.SERVER);
	}
	
	public static void setWork(int time) {
		work = time;
	}
	
	private void closeConnection() {
		comm.close();
	}
	
	// generates a unique handle for a new operator
	private long getUniqueHandle() {
		//return HANDLE_COUNTER++;
		return rand.nextLong();
	}
	
	// adds operator to the { handle => op } mapping
	private long addOp(DbIterator op) {
		return addOp(getUniqueHandle(), op);
	}
	
	private long addOp(long handle, DbIterator op) {
		handlesToOps.put(handle, op);
		return handle;
	}
	
	// gets the operator mapped to by handle
	private DbIterator getOp(long handle) {
		return handlesToOps.get(handle);
	}
	
	private void println(String msg) {
		if (tid == null) {
			Debug.println(this.getName() + ": " + msg, 1);
		} else {
			Debug.println(tid + ": " + msg, 1);
		}
	}
	
	/*================= METHODS TO HANDLE THE VARIOUS REQUESTS ==============*/
	private void begin(BeginReq req) {
		tid = req.tid;
		//transType = (req.transType == BeginReq.READ_ONLY) ? TransactionType.READ_ONLY : TransactionType.READ_WRITE;
		transState = TransactionState.PENDING;
		//TODO: set CommHub status.
		// clear handles->ops mapping
		handlesToOps.clear();
		runHandles.clear();
		assert(executor == null);
	}

	// opens a file and returns the tableid
	private long openFile(OpenFileReq req) {
		return Catalog.Instance().getTableId(req.filename);
	}
	
	// @returns the handle for the sequential scan operator	
	private long spawnIndexedSeqScanOp(SpawnIndexedSeqScanReq req) {
		IndexedSeqScan scan = new IndexedSeqScan(tid, (int)req.handle, req.hwm, req.seeDeleted);
		scan.restrictInsertionTime(new Epoch(req.insertLow), new Epoch(req.insertHigh));
		scan.restrictDeletionTime(new Epoch(req.deleteLow));
		return addOp(scan);		
	}

//	private long spawnSeqScanOp(SpawnSeqScanReq req) {
//		SeqScan scan = new SeqScan(tid, (int)req.handle, snapshot);
//		return addOp(scan);
//	}
	
	// the following operators aren't safe for a versioned database	
	
	// @returns the handle for the insert operator
	private long spawnVersionedInsertOp(SpawnVersionedInsertReq req) throws DbException {
		// construct a db iterator over the request's tuple
		TupleDesc td = req.tuple.getTupleDesc();
		List<Tuple> tuples = new LinkedList();
		tuples.add(req.tuple);
		DbIterator ti = new TupleIterator(td, tuples);
		
		// construct the insert operator
		VersionedInsert insert = new VersionedInsert(tid, ti, (int)req.handle);
		return addOp(insert);
	}
//	
//	// @returns the handle for the delete operator
//	private long spawnDeleteOp(SpawnDeleteReq req) {
//		Delete delete = new Delete(tid, getOp(req.handle));
//		return addOp(delete);
//	}
	
	// @returns the handle for the netscan operator
	private long spawnNetScanOp(SpawnNetScanReq req) {
		NetScan scan = new NetScan(tid, req.td, req.port);
		return addOp(scan);
	}
	
	// @returns the handle for the net insert operator
	private long spawnNetInsertOp(SpawnNetInsertReq req) {
		NetInsert insert = new NetInsert(tid, getOp(req.handle), req.host, req.port);
		return addOp(insert);
	}
	
	// @returns the handle fo the filter operator
	private long spawnFilterOp(SpawnFilterReq req) {
		Filter filter = new Filter(req.pred, getOp(req.handle));
		return addOp(filter);
	}
	
//	private long spawnVersionedSeqScanOp(SpawnVersionedSeqScanReq req) {
//		VersionedSeqScan scan = new VersionedSeqScan(tid, (int)req.handle, req.hwm, snapshot);
//		return addOp(scan);
//	}
	
	private long spawnVersionedUpdateOp(final SpawnVersionedUpdateReq req) throws DbException {
		TupleDesc td = req.tuple.getTupleDesc();
		// small hack - we depend on the fact that the tuple info isn't checked
		Tuple oldTuple = new Tuple(td);
		RecordId rid = Utilities.tupleIdToRecordId(req.tupleId, (int)req.handle, td);
		oldTuple.setRecordId(rid);
		TupleIterator ti = new TupleIterator(oldTuple);
	
		UpdateFunction func = new UpdateFunction() {
			public Tuple update(Tuple t) {
				return req.tuple;
			}
		};
	
		VersionedUpdate update = new VersionedUpdate(tid, ti, func);
		return addOp(update);
	}
	
	private long spawnProjectOp(SpawnProjectReq req) {
		Project project = new Project(tid, getOp(req.handle), req.indices);
		return addOp(project);
	}
	
	// all requests within the batch that require handle information have been relatively
	// specified, i.e. if a SpawnOpReq is built on handle 1, we need to replace the handle
	// with the handle returned by the request in batch.requests[1]
	private void batch(BatchReq batch) throws DbException {
		long[] handles = new long[batch.requests.length];
		for (int i = 0; i < batch.requests.length; i++) {
			// need some nice dispatch function here that ... might return 
			// if SpawnOpReq or OpenFile => handle
			// else just do
			Request req = batch.requests[i];
			
			if (!(req instanceof OpenFileReq || req instanceof SpawnOpReq || req instanceof ExecuteReq))
				throw new RuntimeException("Not batchable req: " + req);
			
			// do handle substitution
			if (req instanceof SpawnOpReq) {
				SpawnOpReq spawn = (SpawnOpReq)req;
				spawn.handle = handles[(int)spawn.handle];
			} else if (req instanceof ExecuteReq) {
				ExecuteReq exec = (ExecuteReq)req;
				exec.handle = handles[(int)exec.handle];
			}
			
			// run the request
			if (req instanceof Ackable) {
				long handle = dispatchAndGetAck((Ackable)req);
				handles[i] = handle;
			} else {
				queueOp((ExecuteReq)req);
			}
		}
	}
	
	private void flush() {
		flush = true;
	}
	
	private void prepare(PrepareReq req) {
		if (transState == TransactionState.PENDING)
			transState = TransactionState.PENDING_EXEC;
		// wait for transaction to complete
		if (executor != null)
			waitForExecutor();
			
		if (work != 0) {
			for (int i = 0; i < work; i++) {
				for (int j = 0; j < WORK_UNIT; j++);
			}
//			try {
////				System.out.println("z");
//				sleep(work);
//			} catch (InterruptedException ie) {}
		}
		if (transState == TransactionState.ABORTED) {
			println("Sending NO vote");
			send(new VoteReq(VoteReq.NO));
			transState = TransactionState.ENDED;
		} else {
			VersionManager.Instance().prepare(tid);
			println("Sending YES vote");
			send (new VoteReq(VoteReq.YES));
			transState = TransactionState.IN_DOUBT;
		}
	}
	
	// wait for the execution thread to complete
	private void waitForExecutor() {
		synchronized (this) {
			Debug.println("Waiting for executor", 10);
			notifyAll();
		}
		try {
			Debug.println("Joining executor", 10);
			executor.join();
			Debug.println("Joined!", 10);
			executor = null;
		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
	}
	
	private void prepareToCommit(PrepareToCommitReq req) {
		println("Sending ack");
		BufferPool.Instance().prepareToCommitTransaction(tid);
		send(new OpAck(1));
	}
	
	// blocks until transaction completes, then commits.
	private void commit(CommitReq req) {
		// wait until all runnable operations have committed before committing
		// if prepare phase was skipped
		if (executor != null) {
			if (transState != TransactionState.IN_DOUBT)
				transState = TransactionState.PENDING_EXEC;
			waitForExecutor();
		}
		transState = TransactionState.COMMITTED;
		transState = TransactionState.ENDED;
		VersionManager.Instance().commit(tid, req.epoch);
		
		if (flush) {
			try {
				BufferPool.Instance().flush_all_pages();
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
//			if (BufferPool.Instance().loggingOn())
//				Log.Instance().flushLog();			
			flush = false;
		}
	}

	// called to signify an abort
	// AbortType specifies the what caused the abort
	// AbortSrc specifies which thread aborted and determines whether we should
	// wait for the execution thread.
	void abort(AbortType type, AbortSrc src) {
		transState = TransactionState.ABORTED;
		if (src != AbortSrc.EXECUTOR && executor != null) {
			waitForExecutor();
		}
		if (type != AbortType.LOCAL_TIMEOUT) {
			VersionManager.Instance().abort(tid);
		}
//		transState = TransactionState.ENDED;
	}
	
	// kills off execution thread and then aborts transaction.
	private void abort(AbortReq req) {
		abort(AbortType.COORDINATED, AbortSrc.WORKER);
	}
	
	private void acquireLock(AcquireTableLockReq req) {
		int tableid = Catalog.Instance().getTableId(req.filename);
		LockManager.Instance().acquireRecoveryLock(req.tid, tableid);
		println("Granting table lock on " + req.filename);
		send(new GrantLockAck());
	}
	
	private void releaseLock(ReleaseTableLockReq req) {
		LockManager.Instance().releaseTableLocks(req.tid);
	}
	
	// schedules the requested handles for execution
	private synchronized void queueOp(ExecuteReq req) {
		runHandles.add(req.handle);
		notifyAll();		
		
		if (executor == null) {
			executor = new ExecutionThread(this);
			executor.start();
		}	
	}
	
	// dequeues a db operator for execution
	synchronized DbIterator dequeueOp() {
		while (true) {
			if (transState == TransactionState.ABORTED) {
				return null;
			}
			if (runHandles.size() > 0) {
				long handle = runHandles.remove(0);
				return getOp(handle);
			}
			// no more operations left
			if (transState == TransactionState.PENDING_EXEC) {
				return null;
			}
			
			try {
				wait();
			} catch (InterruptedException e) {}
		}
	}
	
	private void sendOpAck(long handle) {
		OpAck ack = new OpAck(handle);
		send(ack);
	}
	
//	private void send(Object o) {
//		try {
//			out.writeObject(o);
//		} catch (IOException ioe) {
//			println("IOException: Failed to send " + o);
//		}
//	}
	private void send(Request req) {
		try {
			comm.write(req);
		} catch (IOException ioe) {
			println("IOException: Failed to send " + req);
		}		
	}
	
	private long dispatchAndGetAck(Ackable req) throws DbException {
		if (req instanceof OpenFileReq) {
			println(req.toString());
			long tableid = openFile((OpenFileReq)req);
			return tableid;
		} else if (req instanceof SpawnOpReq) {
			long handle;
			if (req instanceof SpawnIndexedSeqScanReq)
				handle = spawnIndexedSeqScanOp((SpawnIndexedSeqScanReq)req);
			else if (req instanceof SpawnFilterReq)
				handle = spawnFilterOp((SpawnFilterReq)req);
			else if (req instanceof SpawnVersionedInsertReq)
				handle = spawnVersionedInsertOp((SpawnVersionedInsertReq)req);
//			else if (req instanceof SpawnDeleteReq)
//				handle = spawnDeleteOp((SpawnDeleteReq)req);
			else if (req instanceof SpawnNetScanReq)
				handle = spawnNetScanOp((SpawnNetScanReq)req);
			else if (req instanceof SpawnNetInsertReq)
				handle = spawnNetInsertOp((SpawnNetInsertReq)req);
//			else if (req instanceof SpawnVersionedSeqScanReq)
//				handle = spawnVersionedSeqScanOp((SpawnVersionedSeqScanReq)req);
			else if (req instanceof SpawnVersionedUpdateReq)
				handle = spawnVersionedUpdateOp((SpawnVersionedUpdateReq)req);
			else if (req instanceof SpawnProjectReq)
				handle = spawnProjectOp((SpawnProjectReq)req);
			else
				throw new RuntimeException("Unrecognized SpawnOpReq " + req.getClass());
			println(req.toString() + " ==> " + handle);
			return handle;
		} else {
			throw new RuntimeException("Unrecognized Ackable: " + req);
		}
	}
	
	// dispatch based on request types
	// current types:
	// BeginReq
	// FileOpenReq
	// SpawnSeqScanReq, SpawnInsertReq, SpawnDeleteReq, SpawnNetInsertReq, SpawnNetScanReq
	// PrepareReq, CommitReq, AbortReq
	private void dispatch(Request req) throws DbException {
		if (!(req instanceof Ackable)) 
			println(req.toString());
			
		if (req instanceof BeginReq) begin((BeginReq)req);
		else if (req instanceof Ackable) {
			long handle = dispatchAndGetAck((Ackable)req);
			println(req.toString() + " ==> " + handle);
			sendOpAck(handle);
		} else if (req instanceof ExecuteReq) queueOp((ExecuteReq)req);
		else if (req instanceof BatchReq) batch((BatchReq)req);
		else if (req instanceof PrepareReq) prepare((PrepareReq)req);
		else if (req instanceof PrepareToCommitReq) prepareToCommit((PrepareToCommitReq)req);
		else if (req instanceof CommitReq) commit((CommitReq)req);
		else if (req instanceof AbortReq) abort((AbortReq)req);
		else if (req instanceof AcquireTableLockReq) acquireLock((AcquireTableLockReq)req);
		else if (req instanceof ReleaseTableLockReq) releaseLock((ReleaseTableLockReq)req);
		else if (req instanceof FlushReq) flush();
		else throw new RuntimeException("Unrecognized Request " + req.getClass());
	}
	
	public void run() {
		Debug.println("Running " + this.getName() + " on " + 
			socket.getLocalAddress() + ":" + socket.getLocalPort() + " => " +
			socket.getInetAddress() + ":" + socket.getPort());
		try {			
			Request req;
//			while (transState != TransactionState.ENDED) {
			while (true) {
				req = (Request)comm.read();
				dispatch(req);
			}
		} catch (EOFException eof) {
			if (transState != TransactionState.ENDED && tid != null) {
				Debug.println("Socket closed before prepare/commit/abort... aborting...");
				abort(AbortType.SOCKET_ERROR, AbortSrc.WORKER);
			}
		} catch (IOException ioe) {
			System.out.println("IOException at WorkerThread.run: " + ioe.getMessage());
			ioe.printStackTrace();
//		} catch (ClassNotFoundException cnfe) {
//			cnfe.printStackTrace();
		} catch (DbException db) {
			db.printStackTrace();
		}
		closeConnection();
		Debug.println("Finished " + this.getName());
	}

	// if aborted locally, need to wait for PREPARE phase to VOTE no
	
	class ExecutionThread extends Thread {
		
		private WorkerThread worker;
		
		public ExecutionThread(WorkerThread worker) {
			super("ExecutionThread for " + worker.getName());
			this.worker = worker;
		}
	
		public void run(DbIterator dbi) throws DbException, TransactionAbortedException {
			try {
				dbi.open();
				dbi.getNext();
			} catch (NoSuchElementException e) {
			}
			dbi.close();
		}
		
		public void run() {
			println("Running " + this.getName());
			DbIterator op = null;
			while ((op = worker.dequeueOp()) != null) {				
				try {
					println("Executing " + op);
					run(op);
					println("Done " + op);
				} catch (TransactionAbortedException tae) {
					println("Timed out on " + op);
					//tae.printStackTrace();
					worker.abort(AbortType.LOCAL_TIMEOUT, AbortSrc.EXECUTOR);
					break;
				} catch (DbException db) {
					db.printStackTrace();
					worker.abort(AbortType.LOCAL_DB_ERROR, AbortSrc.EXECUTOR);
					break;
				}
			}
			println("Finishing " + this.getName());
		}
	}	
}
