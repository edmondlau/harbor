//
//  TransactionCoordinator.java
//  
//
//  Created by Edmond Lau on 1/23/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.net;

import simpledb.*;
import simpledb.requests.*;

import java.io.*;
import java.net.*;
import java.util.*;

public class CoordinatorHub {

	private static CoordinatorHub instance = new CoordinatorHub();

	public static final int COORD_PORT = 11000;
	
	private boolean transactionsBlocked;
	
	public enum GlobalTransactionState { PENDING, IN_DOUBT, COMMITTED, ABORTED, ENDED };

	// a mapping from transaction id to overall transaction status
	private Map<TransactionId, GlobalTransactionState> tidToStatus;
	
	// allow multiple sites to recovery at once -- not tested
	private MultiMap<TransactionId, RequestForwardingThread> syncs;
	
	// allow multiple requests
	private Map<TransactionId, List<RequestResponsePair>> tidToUpdateQueue;
	
	private Map<TransactionId, CoordinatorThread> tidToCoord;
	
	// listens to recovery join requests
	private RecoveryListenerThread recoverer;
	
	private Set<SnapshotOfPendingTransactions> snapshots;
	
	private CoordinatorHub() {
		tidToStatus = new HashMap<TransactionId, GlobalTransactionState>();
		syncs = new MultiMap<TransactionId, RequestForwardingThread>();
		tidToUpdateQueue = new HashMap<TransactionId, List<RequestResponsePair>>();
		tidToCoord = new HashMap<TransactionId, CoordinatorThread>();
		snapshots = new HashSet<SnapshotOfPendingTransactions>();
		start();
	}
	
	public static CoordinatorHub Instance() {
		return instance;
	}
	
	public void start() {
		recoverer = new RecoveryListenerThread();
		recoverer.setDaemon(true);
		recoverer.start();
	}
	
	public synchronized List<InetSocketAddress> getSitesFor(String data) {
		return SiteCatalog.Instance().getSitesFor(data);
	}
	
	// registers a new transaction as pending
	public synchronized void registerTransaction(TransactionId tid, CoordinatorThread thread) {
		updateState(tid, GlobalTransactionState.PENDING);
		tidToCoord.put(tid, thread);
		tidToUpdateQueue.put(tid, new LinkedList<RequestResponsePair>());
	}
	
	// updates the state of the specified transaction
	public synchronized void updateState(TransactionId tid, GlobalTransactionState state) {
		if (state == GlobalTransactionState.ENDED) {
			tidToStatus.remove(tid);
			tidToUpdateQueue.remove(tid);
			tidToCoord.remove(tid);
			// remove tid from all snapshots if any
			Set<SnapshotOfPendingTransactions> expired = new HashSet();
			for (SnapshotOfPendingTransactions snapshot : snapshots) {
				if (snapshot.contains(tid))
					snapshot.remove(tid);
				if (snapshot.isEmpty()) {
					expired.add(snapshot);
					snapshot.notifyDone();
				}
			}
			snapshots.removeAll(expired);
		} else {
			tidToStatus.put(tid, state);
		}
	}
		
	// adds UpdateRequest to the transaction's update queue.
	// if there is a snapshot of pending transactions and the request is relevant,
	// we need to forward the transaction's updates to the recovering site
	public synchronized void registerUpdate(TransactionId tid, Request req, Long responseHandle) {
		for (SnapshotOfPendingTransactions snapshot : snapshots) {
			if (snapshot.contains(tid) &&
				uses(tid, snapshot.getRelations())) {
				joinPendingTransaction(snapshot.getWorker(), tid);	
			}
		}
		
		List<RequestResponsePair> queue = tidToUpdateQueue.get(tid);
		queue.add(new RequestResponsePair(req, responseHandle));
		notifyAll();
	}
	
	public void registerUpdate(TransactionId tid, Request req) {
		registerUpdate(tid, req, null);
	}
	
//	// called before prepare()
//	public void sync(CoordinatorThread coordinator) {
//		TransactionId tid = coordinator.getTransactionId();
//		// need to check if touched my data
//		// TODO: time-sensitve: need to prevent recover from being called in this block.
//		
//		// SYCHRONIZATION BUG HERE
//		Debug.println("Syncs: " + syncs);
//		if (syncs.containsKey(tid)) {
//			Debug.println("Sync'ing");
//			for (RequestForwardingThread thread : syncs.get(tid)) {
//				Debug.println("Found recovering thread " + thread + " while sync'ing " + tid);
//				synchronized (this) {
//					thread.sync();
//					notifyAll();
//				}
//			}
//			
//			for (RequestForwardingThread thread : syncs.get(tid)) {				
//				try {
//					Debug.println("Trying to join " + thread + ", " + thread.getState());
//					thread.join();
//				} catch (InterruptedException ie) {
//					ie.printStackTrace();
//				}
//			}
//			synchronized (this) {
//				Debug.println("Done forwarding queued requests for " + tid);
//				syncs.remove(tid);
//			}
////			coordinator.add
//		}
//	}

	// adds worker to the list of threads sync'ing with tid
	private synchronized void joinPendingTransaction(InetSocketAddress worker, TransactionId tid) {
		// this constructor also adds the worker as a recovered worker to the coordinator
		RequestForwardingThread thread = new RequestForwardingThread(tidToCoord.get(tid), worker);
		syncs.add(tid, thread);
		thread.start();
	}
	
	// recover worker for the specified relations
	private synchronized void recover(InetSocketAddress worker, Set<String> relations, CommLayer recoveryComm) {
		// mark the site as online for all new incoming transactions
		for (String relation : relations) {
			SiteCatalog.Instance().addLiveSiteFor(worker, relation);
		}
		
		Set<TransactionId> joinableTids = new HashSet<TransactionId>();
		
		Debug.println("tid->Status: " + tidToStatus);
		
		for (TransactionId tid : tidToStatus.keySet()) {
			GlobalTransactionState state = tidToStatus.get(tid);
			// determine if relevant
			if (state.equals(GlobalTransactionState.PENDING) || state.equals(GlobalTransactionState.IN_DOUBT)) {
				if (uses(tid, relations)) {
					Debug.println("Joining " + tid);
					joinPendingTransaction(worker, tid);
				} else {
					Debug.println("Maybe join " + tid);
					joinableTids.add(tid);
				}
			}
		}
		
		SnapshotOfPendingTransactions snapshot = 
			new SnapshotOfPendingTransactions(worker, joinableTids, relations, recoveryComm);		
		if (joinableTids.size() > 0) {
			snapshots.add(snapshot);
			Debug.println("Snapshot: " + snapshot);
		} else {
			snapshot.notifyDone();
			Debug.println("No remaining transactions to wait out and see");
		}
	}
	
	// @returns true if a transaction uses any of the relations
	private synchronized boolean uses(TransactionId tid, Set<String> relations) {
		for (String relation : relations) {
			if (uses(tid, relation))
				return true;
		}
		return false;
	}
	
	// @returns true if a transaction uses the specified relation
	// using a relation means that the transaction opened the file and sent
	// some update request
	private synchronized boolean uses(TransactionId tid, String relation) {
		if (tidToUpdateQueue.containsKey(tid)) {
			List<RequestResponsePair> queue = tidToUpdateQueue.get(tid);
			Debug.println("Update Queue: " + queue);
			boolean opens = false;
			for (RequestResponsePair rrp : queue) {
				Debug.println("Scanning " + rrp.request + " for open " + relation);
				if (opens(rrp.request, relation)) {
					opens = true;
					Debug.println("open " + relation + " found");					
					break;
				}
			}
			if (!opens) return false;
			
			for (RequestResponsePair rrp : queue) {
				Debug.println("Scanning " + rrp.request + " for updates");			
				if (containsUpdate(rrp.request))
					return true;
			}
			return false;
		}
		return false;
	}
	
	// @returns true if the specified request or any subrequest within
	//    it is an Update.
	private synchronized boolean containsUpdate(Request req) {
		if (req instanceof ISpawnUpdateReq)
			return true;
		else if (req instanceof BatchReq) {
			BatchReq batch = (BatchReq)req;
			for (int i = 0; i < batch.requests.length; i++) {
				if (batch.requests[i] instanceof ISpawnUpdateReq)
					return true;
			}
		}
		return false;
	}
	
	// @returns true if the request or any batched subrequest within it
	// opens the specified relation
	private synchronized boolean opens(Request req, String relation) {
		if (directlyOpens(req, relation))
			return true;
		else if (req instanceof BatchReq) {
			BatchReq batch = (BatchReq)req;
			for (int i = 0; i < batch.requests.length; i++) {
				if (directlyOpens(batch.requests[i], relation))
					return true;
			}
		}
		return false;
	}
	
	// @returns true if the specified request is an OpenFileReq for the given relation
	private synchronized boolean directlyOpens(Request req, String relation) {
		return (req instanceof OpenFileReq && ((OpenFileReq)req).filename.equals(relation));
	}
	
	// represents the set of pending transactions that a recovering site
	// can potentially join along with the relations that recovering site
	// cares about
	class SnapshotOfPendingTransactions {
		private InetSocketAddress worker;
		private Set<TransactionId> joinableTids;
		private Set<String> joinableRelations;
		private CommLayer recoveryComm;
		
		public SnapshotOfPendingTransactions(InetSocketAddress worker, Set<TransactionId> tids, Set<String> relations, CommLayer recoveryComm) {
			this.worker = worker;
			joinableTids = new HashSet(tids);
			joinableRelations = new HashSet(relations);
			this.recoveryComm = recoveryComm;
		}
		
		public void remove(TransactionId tid) {
			joinableTids.remove(tid);
		}
			
		public InetSocketAddress getWorker() {
			return worker;
		}
		
		public Set<String> getRelations() {
			return joinableRelations;
		}
		
		public boolean contains(TransactionId tid) {
			return joinableTids.contains(tid);
		}
		
		public boolean isEmpty() {
			return joinableTids.size() == 0;
		}
		
		public void notifyDone() {
			try {
				recoveryComm.write(new OpAck(666));
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
		
		public String toString() {
			StringBuffer buf = new StringBuffer();
			buf.append("worker: " + worker + ",");
			buf.append("tids: " + joinableTids + ",");
			buf.append("relations: " + joinableRelations);
			return buf.toString();
		}
	}
	
	class RequestResponsePair {
		public Request request;
		public Long response;
		
		public RequestResponsePair(Request request, Long response) {
			this.request = request;
			this.response = response;
		}
		
		public String toString() {
			return request.toString();
		}
	}
	
	// forwards any queued updates to a recovering worker
	class RequestForwardingThread extends Thread {
		
		private boolean done;
		private CommLayer comm;
		
		private InetSocketAddress address;
		private TransactionId tid;
		private int reqCursor;
		private CoordinatorThread coord;
				
		public RequestForwardingThread(CoordinatorThread thread, InetSocketAddress address) {
			coord = thread;
			this.tid = coord.getTransactionId();
			this.address = address;
			reqCursor = 0;
			
			try {	
//				Socket socket = new Socket(address.getHostName(), address.getPort());
				Socket socket = Utilities.createClientSocket(address);
				comm = new CommLayer(socket, CommLayer.CLIENT);			
			} catch (IOException ioe) {
				Debug.println("IOException during recovery");
				System.exit(-1);
			}
			coord.addRecoveredWorker(address, comm);
//			handleMap = new HashMap<Long, Long>();
		}
		
		public long sendAndReceive(Request req) throws IOException {
			send(req);
			OpAck ack = (OpAck)comm.read();
			return ack.handle;
		}
		
		public void send(Request req) throws IOException {
			comm.write(req);
		}
		
		public InetSocketAddress getAddress() {
			return address;
		}
		
		public CommLayer getComm() {
			return comm;
		}
		
//		public void sync() {
//			sync = true;
//		}
		
		public void consumeQueue(List<RequestResponsePair> updateQueue) throws IOException {
			int size;
			synchronized(CoordinatorHub.Instance()) {
				size = updateQueue.size();
			
				while (size > reqCursor) {
					RequestResponsePair rrp;
					rrp = updateQueue.get(reqCursor);
					Request req = rrp.request;
					Debug.println("Forwarding : " + req);
					if (req instanceof BatchReq || req instanceof BeginReq) {
						send(req);
					} else if (req instanceof PrepareReq){
						send(req);
						// add recovered worker to coordinator
						coord.addRecoveredWorker(address, comm);
						done = true;
						break;
					} else if (req instanceof AbortReq) {
						send(req);
						done = true;
						break;
					} else {
						throw new RuntimeException("Unrecoverable request: " + req);
					}
					reqCursor++;
				}
			}
		}
		
		public void run() {
			Debug.println("Forwarding thread " + this + " for " + tid + " for " + address);
			int reqCursor = 0;
			List<RequestResponsePair> updateQueue = CoordinatorHub.this.tidToUpdateQueue.get(tid);
			try {
				while (!done) {
//					Debug.println(this + "looping");
					consumeQueue(updateQueue);
					
					synchronized(CoordinatorHub.Instance()) {
						if (!done) {
							try {
//								Debug.println(this + " waiting");
								CoordinatorHub.Instance().wait();
//								Debug.println(this + " done waiting");
							} catch (InterruptedException ie) {
								ie.printStackTrace();
							}
						}
					}
				}
			} catch(IOException ioe) {
				System.out.println("IOException during recovery: " + ioe.getMessage());
				//ioe.printStackTrace();
			}
		}
	}
	
	class RecoveryListenerThread extends Thread {
		public void run() {
			try {
				Debug.println("RecoveryListenerThread listening to " + COORD_PORT);
				ServerSocket server = Utilities.createServerSocket(COORD_PORT);
				while(true) {
					Socket socket = Utilities.accept(server);
					Debug.println("Accepting connection on " + 
						socket.getLocalAddress() + ":" + socket.getLocalPort() + " => " +
						socket.getInetAddress() + ":" + socket.getPort());
					CommLayer comm = new CommLayer(socket, true);
					JoinReq req = (JoinReq)comm.read();	
					InetSocketAddress worker = new InetSocketAddress(req.hostname, req.port);
					Set<String> data = new HashSet(req.relations);
					CoordinatorHub.Instance().recover(worker, data, comm);
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}
	}
	
	// how to handle two phased commit
	// CoordThread sends PREPARE message to each worker
	// CoordThread listens for votes from each worker
	//    if all yes - sends COMMIT message to each worker
	//    if one no - sends ABORT message to each worker
	//       no need to wait for all votes as connections will be closed and packets dropped on closeConnections()
	// what if a worker lags?
	// what if a worker aborts before prepare phase?  => it closes the socket to let it know
	// what if socket connection dies? => send abort
	// will a separate thread help in any of these cases?
	
	// the coordinator polls each worker's incoming channel for votes using available()
	//   returns true if all yes
	//   returns false if a no
	// => solves lagging problem
	
	// separate thread listening for abort messages on ABORT_PORT
	//   when would coordinator thread check to see if the operation was aborted?
	//     either at prepare or when a socket closes prematurely

	// Problem:
	// if server closes the socket, client can't detect it.  The server can only read it
	//		by detecting an EOFException.  But what if reading blocks...
	// spawn a thread and timeout?
	// or assume client will send VOTE(NO).  
	// assume if server blocks on an operation (like netscan) that it will crash with a
	//   socket error if the client is dead.

	// listens to the abort port and notifies coordinator threads of any abort
	// messages.
}