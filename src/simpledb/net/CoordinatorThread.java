//
//  CoordinatorThread.java
//  
//
//  Created by Edmond Lau on 1/23/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.net;

import simpledb.*;
import simpledb.requests.*;
import simpledb.version.*;
import simpledb.net.*;
import simpledb.net.CoordinatorHub.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * A CoordinatorThread handles the communication with all the workers.
 */

public class CoordinatorThread extends Thread {

	public enum CommitProtocol { TWO_PC, THREE_PC };

	public static final int COORD_BASE_PORT = 13000;

	protected TransactionId tid;

	// worker sites
	protected List<InetSocketAddress> workers;
	
	// sockets used to send/receive requests/acks to workers
	private Map<InetSocketAddress, Socket> workerToSocket;
	
	// communication layer for workers
	protected Map<InetSocketAddress, CommLayer> workerToComm;
	
	private Map<InetSocketAddress, Vote> workerToVote;
	
	private Map<InetSocketAddress, CommLayer> recoveredWorkers;
	private List<InetSocketAddress> crashedWorkers = new LinkedList<InetSocketAddress>();

	public enum Vote { NONE, YES, NO };
	
	private static CommitProtocol protocol = CommitProtocol.TWO_PC;

	public CoordinatorThread() {
		workers = new ArrayList<InetSocketAddress>();
		workerToSocket = new HashMap<InetSocketAddress, Socket>();
		workerToComm = new HashMap<InetSocketAddress, CommLayer>();
		workerToVote = new HashMap<InetSocketAddress, Vote>();
		recoveredWorkers = new HashMap<InetSocketAddress, CommLayer>();
		SiteCatalog.Instance();
	}
	
	public static void setCommitProtocol(CommitProtocol proto) {
		protocol = proto;
	}
	
	private void resetState() {
		tid = null;
		workers.clear();
		workerToSocket.clear();
		workerToComm.clear();
		workerToVote.clear();
	}

	protected void println(String msg) {
		if (tid != null) {
			Debug.println(tid + ": " + msg);
		} else {
			Debug.println("X: " + msg);
		}
	}
	
	public TransactionId getTransactionId() {
		return tid;
	}

	public void setWorkerConfiguration(List<InetSocketAddress> sites) {
		workers.clear();
		for (InetSocketAddress worker : sites) {
			addWorker(worker);
		}
	}

	public boolean isSameWorkerConfiguration(List<InetSocketAddress> sites) {
		return workers.equals(sites);
	}

	// add workers to this coordinator
	public void addWorker(InetSocketAddress worker) {
		workers.add(worker);
	}
	
	public void addRecoveredWorker(InetSocketAddress worker, CommLayer comm) {
		recoveredWorkers.put(worker, comm);
	}
	
	private void removeCrashedWorker(InetSocketAddress worker) {
		workers.remove(worker);
		workerToSocket.remove(worker);
		workerToComm.get(worker).close();
		workerToComm.remove(worker);
		SiteCatalog.Instance().removeCrashedWorker(worker);
	}

	// initiate socket connections with the CommHubs of each worker
	public void initiateConnections() {
		try {
			int i = 0;
			for (InetSocketAddress worker : workers) {
				Socket socket = Utilities.createClientSocket(worker);
				workerToSocket.put(worker, socket);
				workerToComm.put(worker, new CommLayer(socket, CommLayer.CLIENT));
				i++;
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
			System.exit(-1);
		}

	}

	public void closeConnections() {
		for (CommLayer comm : workerToComm.values()) {
			comm.close();
		}
		tid = null;
	}
	
	// send request to specified worker
	private void sendReq(Request req, InetSocketAddress worker) throws IOException {
		CommLayer comm = workerToComm.get(worker);
		try {		
			comm.write(req);
		} catch (IOException ioe) {
			removeCrashedWorker(worker);
			throw ioe;
		}
	}
	
	// send request to all workers
	protected void sendReqToAll(Request req) throws IOException{
		for (InetSocketAddress worker : workers) {
			sendReq(req, worker);
		}
//		Iterator<InetSocketAddress> workIter;
//		InetSocketAddress worker;
//		
//		for (workIter = workers.iterator(); workIter.hasNext();) {
//			worker = workIter.next();
//			try {
//				sendReq	(req, worker);
//			} catch (IOException ioe) {
//				workIter.remove();
//				removeCrashedWorker(worker);
//			}
//		}
		CoordinatorHub.Instance().registerUpdate(tid, req);
	}
	
	private Request readReq(InetSocketAddress worker) throws IOException {
		try {
			CommLayer comm = workerToComm.get(worker);
			Request req = comm.read();
			if (req == null)
				throw new IOException();
			return req;
		} catch (IOException ioe) {
			removeCrashedWorker(worker);
			throw ioe;
		}
	}
	
//	// send requests, one to each worker
//	private void sendReqs(Request[] reqs) throws IOException {
//		int count = 0;
//		for (InetSocketAddress worker : workers) {
//			sendReq(reqs[count], worker);
//			count++;
//		}
//	}
//	
	// get ack handles, one from each worker
	private long[] getAcks() throws IOException {
		long[] ackHandles = new long[workers.size()];
		for (int i = 0; i < ackHandles.length; i++) {
			ackHandles[i] = getAck(workers.get(i));
		}
		return ackHandles;
	}
	
	// get ack handle from specified worker
	private long getAck(InetSocketAddress worker) throws IOException {
		CommLayer comm = workerToComm.get(worker);
		OpAck ack = (OpAck)comm.read();
		return ack.handle;
	}
	
//	public TransactionId registerTransaction() {
//		tid = new TransactionId();
//		CoordinatorHub.Instance().registerTransaction(tid);
//		return tid;
//	}
	
	// send begin messages to each worker
	public void beginTransaction(TransactionId tid) throws TransactionAbortedException, IOException {
		this.tid = tid;
		CoordinatorHub.Instance().registerTransaction(tid, this);
		BeginReq req = new BeginReq(tid);
		sendReqToAll(req);
	}
	
	// send prepare messages to each worker
	public void prepare() throws IOException {
		PrepareReq prepare = new PrepareReq();
		sendReqToAll(prepare);
	}	
	
//	// sends reqs to all workers, receives acks from all workers, and registers the update
//	public long[] sendAndReceive(SpawnOpReq[] reqs) throws IOException {
//		sendReqs(reqs);
//		
//		long[] acks = getAcks();
//		CoordinatorHub.Instance().registerUpdate(tid, reqs[0], acks[0]);
//		return acks;
//	}
	
	public void distributedCommit() throws TransactionAbortedException, IOException {
		if (protocol.equals(CommitProtocol.TWO_PC)) {
			println("Two Phase Committing");
			twoPhasedCommit();
		} else if (protocol.equals(CommitProtocol.THREE_PC)) {
			println("Three Phase Committing");
			threePhasedCommit();
		} else {
			throw new RuntimeException("Unrecognized protocol");
		}
	}
	
//	// execute two phased commit protocol
//	// @throws TransactionAbortedException if aborts
	public void twoPhasedCommit() throws TransactionAbortedException, IOException {
		CoordinatorHub.Instance().updateState(tid, GlobalTransactionState.IN_DOUBT);
		prepare();

		boolean votes = getVotes();
		
		if (votes) {
			println("Committing based on votes");
			commit();
		} else {
			println("Aborting based on votes");
			throw new TransactionAbortedException("2PC failed");
		}
	}
	
	public void threePhasedCommit() throws TransactionAbortedException, IOException {
		CoordinatorHub.Instance().updateState(tid, GlobalTransactionState.IN_DOUBT);
		prepare();

		boolean votes = getVotes();
		if (!votes) {
			println("Aborting based on votes");
			throw new TransactionAbortedException("3PC failed");			
		}
		
		println("Preparing to commit");
		prepareToCommit();
		println("Getting acks");
		getAcks();
		
		println("Committing based on votes");
		commit();	
	}
	
	public boolean getVotes() throws IOException {
		workerToVote.clear();
		crashedWorkers.clear();
		boolean no = false;

		for (InetSocketAddress worker : workers) {
			if (!workerToVote.containsKey(worker)) {
				CommLayer comm = workerToComm.get(worker);							
				VoteReq req = (VoteReq)comm.read();
				if (req == null || req.vote == VoteReq.NO) {
					workerToVote.put(worker, Vote.NO);
					no = true;						
					if (req == null)
						crashedWorkers.add(worker);
				} else if (req.vote == VoteReq.YES) {
					workerToVote.put(worker, Vote.YES);
				} else {
					println("Unrecognized vote type");
				}
			}
		}
		
		for (InetSocketAddress crashedWorker : crashedWorkers) {
			removeCrashedWorker(crashedWorker);
		}
		
		// this doesn't support multiple recovered workers
		if (recoveredWorkers.size() != 0) {
			Debug.println("Found recovered worker");
			for (InetSocketAddress worker : recoveredWorkers.keySet()) {
				workers.add(worker);
				CommLayer comm = recoveredWorkers.get(worker);
				workerToSocket.put(worker, comm.getSocket());
				workerToComm.put(worker, comm);
				// collect vote
				VoteReq req = (VoteReq)comm.read();
				if (req == null || req.vote == VoteReq.NO) {
					workerToVote.put(worker, Vote.NO);
					no = true;						
				} else if (req.vote == VoteReq.YES) {
					workerToVote.put(worker, Vote.YES);
				} else {
					println("Unrecognized vote type");
				}
			}
			recoveredWorkers.clear();
		}
		println("Votes: " + workerToVote);
		return !no;
	}
	
	protected void prepareToCommit() throws IOException {
		Epoch time = TimestampAuthority.Instance().getTime();
		PrepareToCommitReq prep = new PrepareToCommitReq(time.value());
		sendReqToAll(prep);	
	}
	
	// send commit messages to each worker
	protected void commit() throws IOException {
		CoordinatorHub.Instance().updateState(tid, GlobalTransactionState.COMMITTED);	
		Epoch time = TimestampAuthority.Instance().getTime();
		CommitReq commit = new CommitReq(time.value());
		VersionManager.Instance().commit(tid, time.value());
		sendReqToAll(commit);
		CoordinatorHub.Instance().updateState(tid, GlobalTransactionState.ENDED);			
	}
	
	// send abort messages to each worker
	// deletes transaction state....
	protected void abort() throws IOException {
		CoordinatorHub.Instance().updateState(tid, GlobalTransactionState.ABORTED);
		AbortReq abort = new AbortReq();
		VersionManager.Instance().abort(tid);
		sendReqToAll(abort);
		CoordinatorHub.Instance().updateState(tid, GlobalTransactionState.ENDED);			
	}
	
	// CONTROLLER THREAD + EXECUTION THREAD + SHARED ABORT THREAD
	// TODO: need to spawn one thread to listen to ABORT_PORT
	// TODO: need to spawn another netscan thread to listen to incoming port?
	// MAIN THREAD will loop and wait for data to finish
	// QUES: when should coordinator thread check status port? at prepare? no too late
}
