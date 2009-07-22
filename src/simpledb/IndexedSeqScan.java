//
//  IndexedSeqScan.java
//  
//
//  Created by Edmond Lau on 3/1/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb;

import java.util.*;
import simpledb.version.*;

/**
 * IndexedSeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class IndexedSeqScan implements DbIterator {

	public static final int NO_DELETE_RESTRICTION = -1;

	private TransactionId tid;
	private int tableId;

	private SegmentedHeapFile dbFile;
	private TupleDesc tupleDesc;

	private Iterator<Tuple> tupleIterator;
	private boolean initialized;
	
	private boolean seeDeleted;
	private int hwm;
	private boolean historical;
	
	private Epoch insertLow, insertHigh, deleteLow;
	
	private List<Segment> segments;
	
	private int pageCursor; // pointer to current page
	private int segStart;   // page where the current segment starts
	private int segEnd;		// page where the current segment ends
	private int segCursor;  // pointer to current segment
	
	public IndexedSeqScan(TransactionId tid, int tableid, int hwm, boolean seeDeleted) {
		// some code goes here
		this.tid = tid;
		tableId = tableid;
		this.hwm = hwm;
		this.seeDeleted = seeDeleted;
		dbFile = (SegmentedHeapFile)Catalog.Instance().getDbFile(tableid);
		tupleDesc = Catalog.Instance().getTupleDesc(tableid);
		
		
		if (hwm == -1) {
			historical = false;
		} else {
			historical = true;
		}
		
		pageCursor = segStart = segEnd = segCursor = -1;
	}
	
	public IndexedSeqScan(TransactionId tid, int tableid, int hwm) {
		this(tid, tableid, hwm, false);
	}
	
	public IndexedSeqScan(TransactionId tid, int tableid, boolean seeDeleted) {
		this(tid, tableid, -1, seeDeleted);
	}	
	
	public IndexedSeqScan(TransactionId tid, int tableid) {
		this(tid, tableid, -1, false);
	}
	
	// includes lower and upper bound in scan
	public void restrictInsertionTime(Epoch low, Epoch high) {
		insertLow = low;
		insertHigh = high;
		assert(insertLow.value() <= insertHigh.value());
	}
	
	// includes lower bound in scan
	public void restrictDeletionTime(Epoch low) {
		deleteLow = low;
	}
	
	private int getNextPageCursor(int cursor) {
		assert(pageCursor == segEnd);
	
		segCursor++;
	
		for (; segCursor < segments.size(); segCursor++) {
			// the last deletion was less than what we require
			if (deleteLow != null && deleteLow.value() > segments.get(segCursor).lastDeletionEpoch.value())
				continue;
				
			// need to find a segment that meets the requirements
			if (insertLow != null && insertHigh != null) {
				int epoch = segments.get(segCursor).startEpoch.value();
				
				if (segCursor + 1 < segments.size()) {
					int nextStartEpoch = segments.get(segCursor+1).startEpoch.value();
					// check that insertLow is not greater than than the end of this segment
					if (insertLow.value() > nextStartEpoch)
						continue;
				}
				
				// check that this insertHigh is not less than the start of this segment
				if (insertHigh.value() < epoch)
					continue;
			}
			
			break;
		}
		
		if (segCursor >= segments.size()) { // all done
			segStart = segEnd = dbFile.numPages();
		} else { // we've found a valid segment
			segStart = segments.get(segCursor).startPageno;
			if (segCursor < segments.size() - 1)
				segEnd = segments.get(segCursor + 1).startPageno - 1;
			else
				segEnd = dbFile.numPages() - 1;
		}
		//System.out.println("segStart: " + segStart + " segEnd: " + segEnd + " segCursor: " + segCursor);
		return segStart;
	}

	/**
	* Opens this sequential scan.
	* Needs to be called before getNext().
	*/
	public void open() throws DbException, NoSuchElementException, TransactionAbortedException {
		// some code goes here
		initialized = true;
		
		SegmentHeaderPage page;
		
		if (historical)
			page = (SegmentHeaderPage)BufferPool.Instance().getPage(tid, dbFile.getHeaderId(), Permissions.READ_ONLY, false);
		else
			page = (SegmentHeaderPage)BufferPool.Instance().getPage(tid, dbFile.getHeaderId(), Permissions.READ_ONLY);

		segments = page.getSegments();
		
		pageCursor = getNextPageCursor(pageCursor);
		if (pageCursor < dbFile.numPages()) {
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

	public Tuple getNext() throws NoSuchElementException, TransactionAbortedException {
		
		Tuple tuple;
		
		while (true) {
			tuple = _getNext();
			IntField insert = (IntField)tuple.getField(VersionManager.INSERTION_COL);
			IntField delete = (IntField)tuple.getField(VersionManager.DELETION_COL);
			if (historical) {
				if (insert.val() > hwm)
					continue;
				if (!seeDeleted && delete.val() <= hwm)
					continue;
				if (delete.val() > hwm) {
					Tuple newTuple = new Tuple(tuple);
					newTuple.setRecordId(tuple.getRecordId());
					newTuple.setField(VersionManager.DELETION_COL, IntField.createIntField(0));
					tuple = newTuple;
				}
				return tuple;
			} else {
				if (!seeDeleted && delete.val() != 0)
					continue;
				return tuple;
			}
		}
	}

	/**
	* Implementation of DbIterator.getNext method.
	* Return the next tuple in the scan.
	* @throws NoSuchElementException if the scan is complete. 
	*/
	public Tuple _getNext() throws NoSuchElementException, TransactionAbortedException {
		// some code goes here
		if (!initialized) {
			// a little defensive coding
			throw new RuntimeException("Called getNext() before calling open()");
		}
				
		if (tupleIterator == null) {
			throw new NoSuchElementException("Empty table");
		}
	
		while (pageCursor < dbFile.numPages() - 1 &&
			   !tupleIterator.hasNext()) {
			if (pageCursor < segEnd) {
				tupleIterator = getIteratorForPage(++pageCursor);
			} else {
				pageCursor = getNextPageCursor(pageCursor);
				if (pageCursor < dbFile.numPages())
					tupleIterator = getIteratorForPage(pageCursor);
			}
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
		pageCursor = segStart = segEnd = segCursor = -1;
		open();
	}
  
  private Iterator<Tuple> getIteratorForPage(int i) throws TransactionAbortedException {
	Page page;
	if (historical) {
		page = BufferPool.Instance().getPage(tid, new PageId(tableId, i), Permissions.READ_ONLY, false);
	} else {
		page = BufferPool.Instance().getPage(tid, new PageId(tableId, i), Permissions.READ_ONLY);
	}
	return page.iterator();
  }

}

