/**
 * Created on Oct 9, 2005
 * Author: Edmond Lau
 */
package simpledb;

import java.util.*;

public class LockManager {

	public static int DEBUG_LOCK = 10;
	public static int DEBUG_LOCK_STATUS = 11;
	public static int DEBUG_TABLE_LOCK = 15;

	private static LockManager instance = new LockManager();
	private static long TIMEOUT = 100000; // milliseconds
	private static int RANDOM_TIMEOUT = 1000; // 400 milliseconds
	private static Random random = new Random();
	
	// invariant - there is no mapping to an empty set
	private Map<TransactionId, Set<Lock>> tidsToLocks;
	private Map<Lock, Set<TransactionId>> locksToTids;
	
	private Map<Integer, TableAccessManager> tableidToManager;
	private Map<TransactionId, Set<Integer>> tidToTableids;
	
	public static LockManager Instance() {
		return instance;
	}
	
	private LockManager() {
		tidsToLocks = new HashMap<TransactionId, Set<Lock>>();
		locksToTids = new HashMap<Lock, Set<TransactionId>>();
		tableidToManager = new HashMap<Integer, TableAccessManager>();
		tidToTableids = new HashMap<TransactionId, Set<Integer>>();
	}
	
	private void printState() {
		Debug.println("\ttidsToLocks: " + tidsToLocks, DEBUG_LOCK_STATUS);
//		Debug.println("\tlocksToTids: " + locksToTids, DEBUG_LOCK);
	}
	
	/**
	 * Waits up to timeRemaining ms.
	 * @return the number of ms left.
	 */
	public long waitUpTo(long timeRemaining) {
		try {
			// timeout code
			long startTime = (new Date()).getTime();
			wait(timeRemaining);
			long endTime = (new Date()).getTime();
			timeRemaining -= (endTime - startTime);
			return timeRemaining;
		} catch (InterruptedException ie) {
			ie.printStackTrace();
			return 0;
		}
	}
	
	public void acquireRecoveryLock(TransactionId tid, int tableid) {
		try {
			acquireTableLock(tid, tableid, Permissions.EXCLUSIVE, Long.MAX_VALUE);
		} catch (TransactionAbortedException tae) {
			System.out.println("shouldn't happen");
			tae.printStackTrace();
		}
	}
	
	// @return time remaining from timeout budget
	public synchronized long acquireTableLock(TransactionId tid, int tableid, Permissions perm, long timeout) throws TransactionAbortedException {
		TableAccessManager manager;
		if (tableidToManager.containsKey(tableid)) {
			manager = tableidToManager.get(tableid);
		} else {
			manager = new TableAccessManager(tableid);
			tableidToManager.put(tableid, manager);
		}
		
		Debug.println(manager.toString(), DEBUG_TABLE_LOCK);
		long timeRemaining = manager.acquireLock(tid, perm, timeout);
		mapAddToSet(tidToTableids, tid, tableid);
		return timeRemaining;
	}
	
	private void mapAddToSet(Map map, Object key, Object value) {
		Set set;
		if (map.containsKey(key)) {
			set = (Set)map.get(key);
		} else {
			set = new HashSet();
			map.put(key, set);
		}
		set.add(value);
	}
	
	public boolean hasTableAccess(TransactionId tid, int tableid, Permissions perm) {
		if (tableidToManager.containsKey(tableid)) {
			TableAccessManager manager = tableidToManager.get(tableid);
			return manager.hasAccess(tid, perm);
		}
		return false;
	}
	
	public synchronized void releaseTableLocks(TransactionId tid) {
		for (Integer tableid : tidToTableids.get(tid)) {
			TableAccessManager manager = tableidToManager.get(tableid);
			manager.releaseLock(tid);
			Debug.println(manager.toString(), DEBUG_TABLE_LOCK);
		}
		tidToTableids.remove(tid);
	}
	
	/**
	 * Acquires a lock on a shared or exclusive lock on a page if the page is not 
	 * currently locked; otherwise, waits for the lock on the page to be released.
	 * @param tid
	 * @param pid
	 * @param perm Determines if the lock is to be a shared lock or exclusive lock.
	 */
	public synchronized void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
		Debug.println(tid + ": acquiring " + perm + " lock on " + pid + "...", DEBUG_LOCK);

		// return from function if lock already held
		assert(!hasAccess(tid, pid, perm));
	
		long timeBudget = TIMEOUT;
	
		if (!hasTableAccess(tid, pid.tableid(), Permissions.SHARED)) {
			timeBudget = acquireTableLock(tid, pid.tableid(), Permissions.SHARED, timeBudget);
		}
		
		Lock exclusiveLock = new Lock(pid, Permissions.READ_WRITE);
		Lock sharedLock = new Lock(pid, Permissions.READ_ONLY);
		
		long timeRemaining = timeBudget;// + random.nextInt(RANDOM_TIMEOUT);
		
		// XXX implement timeout -- do we release all locks or throw TransactionAbortedException or both?
		if (perm == Permissions.READ_WRITE) {
			while (locksToTids.containsKey(exclusiveLock) ||
					(locksToTids.containsKey(sharedLock) && lockHeldByOtherTransactions(sharedLock, tid))) {
				// code to handle dead threads
//				if (locksToTids.containsKey(sharedLock)) {
//					cleanupDeadThreadsOn(sharedLock);
//				} else {
//					cleanupDeadThreadsOn(exclusiveLock);
//				}
				timeRemaining = waitUpTo(timeRemaining);
				if (timeRemaining <= 0) {
					throw new TransactionAbortedException("timed out");
				}
			}
			// if we're upgrading, remove the mappings for the old shared lock
			if (locksToTids.containsKey(sharedLock)) {
				locksToTids.remove(sharedLock);
				tidsToLocks.get(tid).remove(sharedLock);
			}
			
		} else if (perm == Permissions.READ_ONLY) {
			while (locksToTids.containsKey(exclusiveLock)) {
//				cleanupDeadThreadsOn(exclusiveLock);
				
				timeRemaining = waitUpTo(timeRemaining);
				if (timeRemaining <= 0) {
					throw new TransactionAbortedException("timed out");
				}
			}
		}
		
		// we're good to acquire a lock
		Lock lock = (perm == Permissions.READ_ONLY) ? sharedLock : exclusiveLock;
		
		// add the lock -> Set<Tid> mapping
		mapAddToSet(locksToTids, lock, tid);
//		Set<TransactionId> tidSet;
//		if (locksToTids.containsKey(lock)) {
//			tidSet = locksToTids.get(lock);
//		} else {
//			tidSet = new HashSet<TransactionId>();
//			locksToTids.put(lock, tidSet);
//		}
//		tidSet.add(tid);
		
		// add the Tid -> Set<Lock> mapping
//		Set<Lock> lockSet;
//		if (tidsToLocks.containsKey(tid)) {
//			lockSet = tidsToLocks.get(tid);
//		} else {
//			lockSet = new HashSet<Lock>();
//			tidsToLocks.put(tid, lockSet);
//		}
//		lockSet.add(lock);
		mapAddToSet(tidsToLocks, tid, lock);
		
		Debug.println(tid + ": granted " + perm + " lock on " + pid, DEBUG_LOCK);
		printState();
	}
	
	/**
	 * Releases a lock on a page.
	 * @param tid
	 * @param pid
	 */
	public synchronized void releaseLock(TransactionId tid, PageId pid) {
		Debug.println(tid + ": releasing lock on " + pid + "...", DEBUG_LOCK);
		Lock exclusiveLock = new Lock(pid, Permissions.READ_WRITE);
		Lock sharedLock = new Lock(pid, Permissions.READ_ONLY);
		
		assert(locksToTids.containsKey(exclusiveLock) || locksToTids.containsKey(sharedLock));
		assert(!(locksToTids.containsKey(exclusiveLock) && locksToTids.containsKey(sharedLock)));
		
		Lock lock = (locksToTids.containsKey(exclusiveLock)) ? exclusiveLock : sharedLock;
		
		Set<Lock> lockSet = tidsToLocks.get(tid);
		lockSet.remove(lock);
		if (lockSet.size() == 0) {
			tidsToLocks.remove(tid);
		}
		
		Set<TransactionId> tidSet = locksToTids.get(lock);
		tidSet.remove(tid);
		if (tidSet.size() == 0) {
			locksToTids.remove(lock);
		}
		notify();
		
		printState();
	}
	
	/** 
	 * Releases all locks of the specified transaction.
	 * @param tid
	 */
	public synchronized void releaseLocks(TransactionId tid) {
		Debug.println(tid + ": releasing all locks", DEBUG_LOCK);
		if (tidsToLocks.containsKey(tid)) {
			for (Lock lock : tidsToLocks.get(tid)) {
				removeLockToTidMapping(lock, tid);
			}
			tidsToLocks.remove(tid);
			releaseTableLocks(tid);
			notify();
		}

		printState();
	}
	
	public Iterator<PageId> getLockedPagesIterator(final TransactionId tid) {
		final Iterator<Lock> lockIter;
		if (tidsToLocks.containsKey(tid)) {
			lockIter = tidsToLocks.get(tid).iterator();
		} else {
			lockIter = null;
		}
		
		return new Iterator<PageId>() {
			
			public boolean hasNext() {
				if (lockIter == null) {
					//Debug.println("Page Iterator called for tid " + tid + " w/o lock");
					return false;
				}
				return lockIter.hasNext();
			}
			
			public PageId next() {
				if (lockIter == null) {
					throw new NoSuchElementException("no locked pages by " + tid);
				}
				Lock lock = lockIter.next();
				return lock.getPageId();
			}
			
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
	
	public Permissions lookupAccess(TransactionId tid, PageId pid) {
		if (!tidsToLocks.containsKey(tid)) {
			return null;
		}
		for (Lock lock : tidsToLocks.get(tid)) {
			if (lock.getPageId().equals(pid)) {
				return lock.getPermissions();
			}
		}
		return null;
	}
	
	public boolean hasAccess(TransactionId tid, PageId pid, Permissions perm) {
		Permissions access = lookupAccess(tid, pid);
		return (access != null && access.permLevel >= perm.permLevel);
	}
	
	public boolean isPageLocked(PageId pid) {
		Lock exclusiveLock = new Lock(pid, Permissions.READ_WRITE);
		Lock sharedLock = new Lock(pid, Permissions.READ_ONLY);
		
		return locksToTids.containsKey(exclusiveLock) || locksToTids.containsKey(sharedLock);
	}
	
	private synchronized void removeLockToTidMapping(Lock lock, TransactionId tid) {
		Set<TransactionId> tidSet = locksToTids.get(lock);
		tidSet.remove(tid);
		if (tidSet.size() == 0) {
			locksToTids.remove(lock);
		}
	}
	
	private boolean lockHeldByOtherTransactions(Lock lock, TransactionId tid) {
		assert(locksToTids.containsKey(lock));
		Set<TransactionId> tidSet = locksToTids.get(lock);
		assert(tidSet.size() > 0);
		return !(tidSet.size() == 1 && tidSet.contains(tid));
	}

//	private synchronized TransactionId findDeadThread(Lock lock) {
//		for (TransactionId tid : locksToTids.get(lock)) {
//			if (!tid.getThread().isAlive()) {
//				return tid;
//			}
//		}
//		return null;
//	}
	
//	private synchronized void cleanupDeadThreadsOn(Lock lock) {
//		TransactionId tid;
//		while((tid = findDeadThread(lock)) != null) {
//			BufferPool.Instance().abortTransaction(tid);
//		}
//	}
}
/**
 * A immutable class representing a lock.
 * @author Edmond Lau
 * 
 * Lock
 */
class Lock {
	
	private PageId pid;
	private Permissions perm;
	
	public Lock(PageId pid, Permissions perm) {
		this.pid = pid;
		this.perm = perm;
	}
	
	public PageId getPageId() {
		return pid;
	}
	
	public Permissions getPermissions() {
		return perm;
	}
	
	public boolean equals(Object o) {
		if (!(o instanceof Lock))
			return false;
		Lock lock = (Lock)o;
		return lock.pid.equals(pid) && lock.perm.equals(perm);
	}
	
	public int hashCode() {
		return pid.hashCode() + perm.hashCode();
	}
	
	public String toString() {
		return perm.toString() + pid.toString();
	}
}

	// state to capture:
	// which transactions have shared lock on a table
	// which transaction has exclusive lock on a table
	// which tables have a pending exclusive lock
	// how about? tableQueue, tableLocks.
	// table lock - shared: set tids, exclusive: tid, queue: 

class TableAccessManager {

	private int tableid;
	private Set<TransactionId> sharedHolders;
	private TransactionId exclusiveHolder;
	private List<TransactionId> queue;
	
	public TableAccessManager(int tableid) {
		this.tableid = tableid;
		sharedHolders = new HashSet<TransactionId>();
		exclusiveHolder = null;
		queue = new LinkedList<TransactionId>();
	}
	
	public int getTableId() {
		return tableid;
	}
	
	public boolean hasAccess(TransactionId tid, Permissions perm) {
		return (perm == Permissions.EXCLUSIVE && exclusiveHolder == tid) ||
			   (perm == Permissions.SHARED && (exclusiveHolder == tid || sharedHolders.contains(tid)));
	}
	
	// @returns time remaining
	public long acquireLock(TransactionId tid, Permissions perm, long timeout) throws TransactionAbortedException {
		if (isGrantable(perm)) {
			grant(tid, perm);
			return timeout;
		}
		
		long timeRemaining = timeout;
		
		queue.add(tid);
		while (timeRemaining > 0) {
			try {
				// timeout code
				long startTime = (new Date()).getTime();
				synchronized (LockManager.Instance()) {
					Debug.println(tid + ": Waiting for " + perm + " table lock on " + tableid, LockManager.DEBUG_LOCK);
					LockManager.Instance().wait(timeRemaining);
				}
				long endTime = (new Date()).getTime();
				timeRemaining -= (endTime - startTime);
			} catch (InterruptedException ie) {
				ie.printStackTrace();
				break;
			}
			
			if (isGrantable(perm) && queue.get(0) == tid) {
				queue.remove(0);
				grant(tid, perm);
				Debug.println(tid + ": Got " + perm + " table lock on " + tableid, LockManager.DEBUG_LOCK);
				return timeRemaining;
			}
		}
		queue.remove(tid);
		throw new TransactionAbortedException("timed out trying to obtain lock on table " + tableid);
	}
	
	public void releaseLock(TransactionId tid) {
		if (exclusiveHolder != null && exclusiveHolder.equals(tid)) {
			exclusiveHolder = null;
		} else {
			sharedHolders.remove(tid);
		}
		synchronized(LockManager.Instance()) {
			LockManager.Instance().notifyAll();
		}
	}
	
	private boolean isGrantable(Permissions perm) {
		return (perm == Permissions.EXCLUSIVE && exclusiveHolder == null && sharedHolders.size() == 0) ||
			   (perm == Permissions.SHARED && exclusiveHolder == null);
	}
	
	private void grant(TransactionId tid, Permissions perm) {
		if (perm == Permissions.EXCLUSIVE) {
			exclusiveHolder = tid;
		} else if (perm == Permissions.SHARED) {
			sharedHolders.add(tid);
		}
	}
	
	public String toString() {
		StringBuffer buf = new StringBuffer();
		buf.append("TableId: " + tableid + "\n");
		buf.append("sharedHolders: " + sharedHolders + "\n");
		buf.append("exclusiveHolder: " + exclusiveHolder + "\n");
		buf.append("queue: " + queue + "\n");
		return buf.toString();
	}
}
