package simpledb;

import simpledb.log.*;
import java.util.*;
import java.io.*;
import simpledb.version.*;

/**
 * The BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches pages from
 * the appropriate location.
 * 
 * Locking role:
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool will check that the transaction has the appropriate
 * locks to read/write the page.
 *
 * Recovery role:
 * The BufferPool currently enforces a NO-FORCE/STEAL policy.  Therefore, it is
 * necessary to undo changes from aborted transactions and either use a log or 
 * remote recovery to redo lost transactions.  The BufferPool is responsible
 * for scheduling checkpoints either at fixed timer intervals or after a specified
 * number of transactions.  To support the ARIES protocol, it maintains a dirty
 * pages table a la ARIES.  The ARIES transaction table is maintained by the Log.
 */
public class BufferPool {
  /** Bytes per page, excluding header. */
  public static final int PAGE_SIZE = 4096;
  private static final int NUM_PAGES = 1024 * 25 * 5; // 100 MB buffer pool
  private static final BufferPool _instance = new BufferPool(NUM_PAGES);
  
	private static long FLUSH_FREQ = Long.MAX_VALUE; // 1 flush per this many ms
	private static long ARIES_CHKPT_FREQ = 1000;
	
	private int nTransactions;
  
  // the buffer pool policy
  private boolean steal; // if steal is true, we must either use logging or 
						 // versioning to preserve correctness
  private boolean force; // if no-force, we must either use logging or versioning
						 // to support aborts
 
  private Integer dirtyPageLatch = new Integer(1);
  
  private Map<PageId, Page> pageIdsToPages;
  private Map<PageId, Long> dirtyPages;
  
  private int numPages;
  
  private boolean logging;
  
  private Random rand;
  
  private Timer ariesChkptTimer;
  private Timer flushTimer;
  
  private LogFlushThread logFlushThread;
  private boolean groupCommit;
  
  /**
   * Access to BufferPool instance.
   */
  public static BufferPool Instance() {
    return _instance;
  }

  /**
   * Constructor.
   * @param numPages number of pages in this buffer pool 
   */
  private BufferPool(int numPages) {
    // some code goes here
  	this.numPages = numPages;
  	pageIdsToPages = new HashMap<PageId, Page>(NUM_PAGES);
	dirtyPages = new HashMap<PageId, Long>();
	logging = false;
	groupCommit = true;
	rand = new Random();
	steal = true;
	force = false;
	// pset settings
//	steal = false;
//	force = true;
  }
  
  // turns logging on/off
  public void setLogging(boolean logging) {
	this.logging = logging;
	if (logging && ariesChkptTimer == null) {
		ariesChkptTimer = new Timer(true); // sets as a daemon thread
		TimerTask task = 
			new TimerTask() {
				public void run() {
//					writeCheckpointHook();
					Log.Instance().logCheckpoint();
				}
			};
		ariesChkptTimer.schedule(task, ARIES_CHKPT_FREQ, ARIES_CHKPT_FREQ);		
	}
	if (logging && logFlushThread == null) {
		logFlushThread = new LogFlushThread();
		logFlushThread.start();
	}
  }
  
  public boolean loggingOn() {
	return logging;
  }
  
  public void setFlushFreq(long interval) {
	FLUSH_FREQ = interval;
	if (flushTimer != null) {
		flushTimer.cancel();
	}
	flushTimer = new Timer(true); // sets as a daemon thread
	
	TimerTask task = 
		new TimerTask() {
			public void run() {
				writeCheckpointHook();
			}
		};
		
	flushTimer.schedule(task, FLUSH_FREQ, FLUSH_FREQ);	
  }
  
  public void setGroupCommit(boolean setting) {
	groupCommit = setting;
  }
  
  // sets the recovery lsn of the given page id.
  // used for ARIES recovery.
  public void setRecoveryLsn(PageId pid, long recLsn) {
	LatchManager.Instance().latch(dirtyPageLatch, Permissions.EXCLUSIVE);
	dirtyPages.put(pid, recLsn);
	LatchManager.Instance().unlatch(dirtyPageLatch);
  }
  
  // @return a snapshot of the dirty pages table
  public Map<PageId, Long> getDirtyPages() {
	LatchManager.Instance().latch(dirtyPageLatch, Permissions.SHARED);  
	Map m = new HashMap(dirtyPages);
	LatchManager.Instance().unlatch(dirtyPageLatch);
	return m;
  }
  
  /**
   * Retrieve the specified page with the associated permissions.
   * Set acquireLocks to false and perm to READ_ONLY for snapshot isolation.
   * Will acquire a lock and may block if that lock is held by another
   * transaction.
   */
  public Page getPage(TransactionId tid, PageId pid, Permissions perm, boolean acquireLocks) throws TransactionAbortedException {
//	Debug.println("Getting page " + pid + " with " + perm, 10);
	// some code goes here
//	if (!acquireLocks && !perm.equals(Permissions.READ_ONLY)) {
//		abortTransaction(tid);
//		throw new TransactionAbortedException("Tried to run snapshot isolation w/ READ_WRITE query");
//	}
	if (acquireLocks) {
		try {
			if (!LockManager.Instance().hasAccess(tid, pid, perm)) {
				LockManager.Instance().acquireLock(tid, pid, perm);
			}
		} catch (TransactionAbortedException tae) {
			// abort transaction by releasing locks and removing dirty pages from pool
			abortTransaction(tid);
			throw tae;
		}
	}
	
	Page page;
	
	synchronized(this) {
	
		// is the page in the buffer pool?
		if (pageIdsToPages.containsKey(pid)) {
			page = pageIdsToPages.get(pid);		
			// for ARIES checkpointing purposes
			if (logging && !page.isDirty() && perm.equals(Permissions.READ_WRITE))
				setRecoveryLsn(pid, Log.Instance().endOfLog());
			return page;
		}
		
		if (pageIdsToPages.size() == numPages) {
			if (!evictPage()) {
				abortTransaction(tid);
				throw new RuntimeException("Unable to evict a page -- Increase the buffer pool size");
			}
		}
		
		DbFile dbFile = Catalog.Instance().getDbFile(pid.tableid());
		page = dbFile.readPage(pid);
		pageIdsToPages.put(pid, page);
	}
	
	// for ARIES checkpointing purposes
	if (logging && !page.isDirty() && perm.equals(Permissions.READ_WRITE))
		setRecoveryLsn(pid, Log.Instance().endOfLog());
	
	return page;	
  }

  /**
   * Retrieve the specified page with the associated permissions.
   * Will acquire a lock and may block if that lock is held by another
   * transaction.
   */
  public Page getPage(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
	return getPage(tid, pid, perm, true);
  }

  /**
   * Releases the lock on a page.
   * Calling this is very risky, and may result in wrong behavior. Think hard
   * about who needs to call this and why, and why they can run the risk of
   * calling it.
   */
  public void releasePage(TransactionId tid, PageId pid) {
  	// I think this call is ok when we're scanning the file to look
  	// for an empty slot to insert to, and we weren't holding the lock previously
    // some code goes here
	// hack for segments
  	LockManager.Instance().releaseLock(tid, pid);
  }

  /**
   * Release all locks associated with a given transaction.
   */
  public void transactionComplete(TransactionId tid) {
    // some code goes here
  	// flush all dirty pages written by transaction
  	// release all locks
  	commitTransaction(tid);
  }

  /**
   * Add a tuple to the specified table behalf of transaction tid.
   * Will acquire a write lock on the page the tuple is added to. May block if
   * the lock cannot be acquired.
   * @return RecordId the RecordId of the newly inserted tuple
   * @param tid the transaction adding the tuple
   * @param tableId the table to add the tuple to
   * @param t the tuple to add
   */
  public RecordId insertTuple(TransactionId tid, int tableId, Tuple t)
    throws DbException, IOException, TransactionAbortedException
  {
    // some code goes here
  	DbFile dbFile = Catalog.Instance().getDbFile(tableId);
  	TupleDesc tupleDesc = Catalog.Instance().getTupleDesc(tableId);
  	if (!tupleDesc.equals(t.getTupleDesc())) {
		Debug.println("Tuple Desc: " + t.getTupleDesc());
		Debug.println("Table's Tuple Desc: " + tupleDesc);
  		throw new DbException("Table " + tableId + " has schema " + tupleDesc + 
  				", but tried to add tuple with schema " + t.getTupleDesc());
  	}
  	RecordId rid = dbFile.addTuple(tid, t);
	assert(tableId == rid.tableid());

	Page page;
	synchronized (this) {
		page = pageIdsToPages.get(new PageId(rid.tableid(), rid.pageno()));
	}
	page.markDirty(true);
	return rid;

  }

  /**
   * Remove the specified tuple from the buffer pool.
   * Will acquire a write lock on the page the tuple is deleted from. May block if
   * the lock cannot be acquired.

   * @param tid the transaction deleting the tuple.
   * @param t the tuple to delete
   */
  public void deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
    // some code goes here
  	RecordId rid = t.getRecordId();
  	
  	DbFile dbFile = Catalog.Instance().getDbFile(rid.tableid());
  	Page page = dbFile.deleteTuple(tid, t);
  	page.markDirty(true);
  }
  
  /**
   * Sets the tuple identified by rid to func(tuple).
   * Will acquire a lock on the page the tuple is updated from.
   */
  public void updateTuple(TransactionId tid, RecordId rid, UpdateFunction func) throws DbException, TransactionAbortedException {
	DbFile dbFile = Catalog.Instance().getDbFile(rid.tableid());
	Page page = dbFile.updateTuple(tid, rid, func);
	page.markDirty(true);
  }

  /**
   * Flush all dirty pages to disk.
   * NB: THIS IS PURELY FOR TESTING PURPOSES.  YOU SHOULD NOT NEED TO
   * CALL THIS FUNCTION AS IT WILL BREAK THE TRANSACTION SYSTEM IN SIMPLEDB.  
   * The only reason this exists is so that the tester
   * can force pages to disk.  
   */
  public void flush_all_pages() throws IOException {
	if (logging)
		Log.Instance().flushLog();  
	synchronized(this) {
		flush_all_pages(pageIdsToPages);
	}
  }
  
  public void flush_all_pages(Map<PageId, Page> pages) throws IOException {
	for (PageId pid : pages.keySet()) {
  		Page page = pages.get(pid);
//		Debug.println("page: " + pid + " dirty? " + page.isDirty());
  		if (page.isDirty()) {
  			flush_page(pid);
  		}
  	}
  }

  /**
   * Flushes a certain page to disk
   */
  private void flush_page(PageId pid) throws IOException {
    // some code goes here
  	DbFile dbFile = Catalog.Instance().getDbFile(pid.tableid());
	Page page;
	synchronized (this) {
		page = pageIdsToPages.get(pid);
	}
  	dbFile.writePage(page);
  	// mark the pages as clean
  	page.markDirty(false);
	
	LatchManager.Instance().latch(dirtyPageLatch, Permissions.EXCLUSIVE);
	dirtyPages.remove(pid);
	LatchManager.Instance().unlatch(dirtyPageLatch);
  }
  
  private synchronized boolean evictPage()  {
//	System.out.print("e");
  	// at this point, no reason to evict a page until the buffer pool is full
  	if (pageIdsToPages.size() != NUM_PAGES) {
  		throw new RuntimeException("Tried to evict page when buffer pool is non-full");
  	}
	
	// first try to evict a random page
	int pageToEvict = rand.nextInt(NUM_PAGES);
	int curPage = 0;
	
	for (Page page : pageIdsToPages.values()) {
		if (pageToEvict == curPage) {
			// if we have a no steal policy and the page is locked, we can't evict it
			if (!steal && LockManager.Instance().isPageLocked(page.id())) {
				break;
			}
			evictPage(page);
			return true;
		}
		curPage++;
	}
	
	// evict the first page allowed by the ordering
	Debug.println("Trying to evict: " + pageIdsToPages);
  	for (Page page : pageIdsToPages.values()) {
		Debug.println(page.id() + ": L" + LockManager.Instance().isPageLocked(page.id()) + " D" + page.isDirty());
		if (page.id().pageno() < 0)
			continue;
	
		if (!steal && LockManager.Instance().isPageLocked(page.id()))
			continue;
		
		evictPage(page);
		return true;
  	}
	
	return false;
  }
  
  private synchronized void evictPage(Page page) {
	if (page.isDirty()) {
		try {
			flush_page(page.id());
		} catch (IOException ioe) {
			ioe.printStackTrace();
			throw new RuntimeException("Unable to flush page on eviction");
		}
	}
	pageIdsToPages.remove(page.id());	
  }
  
  public void prepareToCommitTransaction(TransactionId tid) {
		if (logging) {
			Log.Instance().logPrepareToCommit(tid);
			// do group commits/flushes
			if (groupCommit)
				logFlushThread.requestFlush();
			else
				Log.Instance().flushLog();
		}
  }
  
  public void prepareTransaction(TransactionId tid) {
		if (logging) {
			Log.Instance().logPrepare(tid);
			// do group commits/flushes
			if (groupCommit)
				logFlushThread.requestFlush();
			else
				Log.Instance().flushLog();
		}
  }
  
  public void commitTransaction(TransactionId tid) {
	try {
		endTransaction(tid, false);
	} catch (IOException ioe) {
		ioe.printStackTrace();
		throw new RuntimeException("I/O error while committing transaction " + tid);		
	}
  }
  
  public void abortTransaction(TransactionId tid) {
  	try {
		if (logging) {
			try {
				Log.Instance().logAbort(tid);
				Log.Instance().rollback(tid);
			} catch (DbException db) {
				db.printStackTrace();
			}
		}
  		endTransaction(tid, true);
  	} catch (IOException ioe) {
  		// this can't happen
  		ioe.printStackTrace();
		throw new RuntimeException("I/O error while aborting transaction " + tid);				
  	}
  }
  
  /**
   * Aborts/commits transaction tid by removing/flushing all of its 
   * dirty pages from the buffer pool and releasing its locks.
   * @param tid
   */
  private void endTransaction(TransactionId tid, boolean abort) throws IOException {
  
	// if we're aborting
  	Iterator<PageId> pidIter = LockManager.Instance().getLockedPagesIterator(tid);
  	while (pidIter.hasNext()) {
  		PageId pid = pidIter.next();
		synchronized(this) {
			if (pageIdsToPages.containsKey(pid)) {
				Page page = pageIdsToPages.get(pid);
				if (page.isDirty()) {
					if (!abort && force) {
						flush_page(pid);
					} else if (abort && force && !steal) {
						pageIdsToPages.remove(pid);
					}
				}
			}
		}
  	}
  	LockManager.Instance().releaseLocks(tid);
	
	// if we're using transaction-based checkpoints, check if it's time for 
	// another checkpoint.
	nTransactions++;
	
	if (logging) {
		Log.Instance().logEnd(tid);
		if (groupCommit)
			logFlushThread.requestFlush();
		else
			Log.Instance().flushLog();		
	}

  }
  
  private void writeCheckpointHook() {
	if (logging) {
		writeCheckpoint();
	} else {
		VersionManager.Instance().writeCheckpoint();
	}
  }  
  
  public void writeCheckpoint() {
//	if (logging) {
//		Log.Instance().logCheckpoint();
//	}// else {
		Map<PageId,Page> m;
		synchronized(this) {
			m = new HashMap(pageIdsToPages);
		}
		try {
			flush_all_pages(m);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
//	}
  }
}
