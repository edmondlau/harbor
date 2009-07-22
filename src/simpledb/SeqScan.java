package simpledb;

import java.util.*;
import simpledb.version.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

	private TransactionId tid;
	private int tableId;

	private DbFile dbFile;
	private TupleDesc tupleDesc;

	private int pageCursor;
	private Iterator<Tuple> tupleIterator;
	private boolean initialized;
	
	private boolean snapshotIsolated;
	
	// snapshot isolation as defined here is broken
	public SeqScan(TransactionId tid, int tableid, boolean snapshotIsolated) {
		// some code goes here
		this.tid = tid;
		tableId = tableid;
		this.snapshotIsolated = snapshotIsolated;
		dbFile = Catalog.Instance().getDbFile(tableid);
		tupleDesc = Catalog.Instance().getTupleDesc(tableid);
		pageCursor = 0;
	}
	
	
  /** Constructor.
   * Creates a sequential scan over the specified table as a part of the
   * specified transaction.
   * @param tid The transaction this scan is running as a part of.
   * @param tableid the table to scan.
   */
  public SeqScan(TransactionId tid, int tableid) {
	this(tid, tableid, false);
  }

  /**
   * Opens this sequential scan.
   * Needs to be called before getNext().
   */
  public void open() throws DbException, NoSuchElementException, TransactionAbortedException {
    // some code goes here
  	initialized = true;
	
  	if (dbFile.numPages() > 0) {
  		tupleIterator = getIteratorForPage(pageCursor);
  	}
  }

  /**
   * Implementation of DbIterator.getTupleDesc method.
   */
  public TupleDesc getTupleDesc() {
    // some code goes here
    return tupleDesc;
  }

  /**
   * Implementation of DbIterator.getNext method.
   * Return the next tuple in the scan.
   * @throws NoSuchElementException if the scan is complete. 
   */
  public Tuple getNext() throws NoSuchElementException, TransactionAbortedException {
    // some code goes here
  	if (!initialized) {
  		// a little defensive coding
  		throw new RuntimeException("Called getNext() before calling open()");
  	}
		
  	if (tupleIterator == null) {
  		throw new NoSuchElementException("Empty table");
  	}
  	// check if any more pages
  	// subtract 1 because 0 indexed
  	// must handle the case where the next page has no more tuples
  	while (pageCursor < dbFile.numPages() - 1 && 
		   !tupleIterator.hasNext()) {
  		tupleIterator = getIteratorForPage(++pageCursor);
  	}

  	if (tupleIterator.hasNext()) {
  		return tupleIterator.next();
  	}
  	
  	throw new NoSuchElementException("End of relation");
  }

  /**
   * Closes the sequential scan.
   */
  public void close() {
    // some code goes here
  	tupleIterator = null;
  	initialized = false;
  }

  /**
   * Rewinds the sequential back to the first record.
   */
  public void rewind() throws DbException, NoSuchElementException, TransactionAbortedException {
    // some code goes here
  	if (!initialized) {
  		throw new RuntimeException("Called rewind() w/o calling calling open()");
  	}
  	pageCursor = 0;
  	if (dbFile.numPages() > 0) {
  		tupleIterator = getIteratorForPage(pageCursor);
  	}
  }
  
  private Iterator<Tuple> getIteratorForPage(int i) throws TransactionAbortedException {
	Page page;
	if (snapshotIsolated) {
		page = BufferPool.Instance().getPage(tid, new PageId(tableId, i), Permissions.READ_ONLY, false);
	} else {
		page = BufferPool.Instance().getPage(tid, new PageId(tableId, i), Permissions.READ_ONLY);
	}
	return page.iterator();
  }

}
