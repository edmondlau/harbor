//
//  Aries.java
//  
//
//  Created by Edmond Lau on 2/6/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.log;

import simpledb.*;
import simpledb.log.LogRecord.*;
import simpledb.log.TransactionTable.*;
import simpledb.test.*;

import java.util.*;
import java.io.*;
import java.text.ParseException;

/**
 * This is an implementation of the ARIES recovery algorithm, described in
 * C. Mohan's paper, "ARIES: A Transaction Recovery Method Supporting 
 * Fine-Granularity Locking and Partial Rollbacks Using Write-Ahead Logging"
 * ( http://portal.acm.org/citation.cfm?id=128770 ).
 * 
 * The implementation relies on the simpledb.log package.
 * Log provides an interface to the on-disk log.
 * LogRecord provides an abstraction to a record written to the log.
 * TransactionTable and DirtyPages support the online bookkeeping maintenance
 * used by the ARIES protocol.  Note that DirtyPages could have been integrated
 * into the BufferPool's pre-existing dirty pages data structure, but it was
 * simpler to create a new structure than to muck around with the existing one.
 */
public class Aries {

	private long masterAddr;
	private TransactionTable transTable;
	private Map<PageId,Long> dirtyPages;
	private long redoLsn;
	
	private Log log;
	
	public Aries(long masterAddr) {
		System.out.println("Last checkpoint: " + masterAddr);
		this.masterAddr = masterAddr;
		transTable = TransactionTable.Instance();
		dirtyPages = new HashMap<PageId,Long>();
		log = Log.Instance();
		redoLsn = -1;
	}

	// runs the analysis phase of recovery
	public void restartAnalysis() throws DbException {
		// initialize transTable and dirtyPages to empty
		transTable.clear();
		dirtyPages.clear();
		
		// open log scan at master rec
		LogRecord rec = log.readLog(masterAddr);
		while (rec != null) {
			// if trans-related and not in transTable
			if (rec.type != RecordType.BEGIN_CHECKPOINT && rec.type != RecordType.END_CHECKPOINT) {
				if (!transTable.containsKey(rec.tid)) {
					// is this correct?
					transTable.insert(rec.tid, rec.lsn, State.UNPREPARED, rec.prevLsn);
				}
			}
			
			TransactionRecord tr;
			switch (rec.type) {
				case UPDATE:
				case INSERT:
				case DELETE:
				case COMPENSATION:
					tr = transTable.get(rec.tid);
					tr.lastLsn = rec.lsn;
					if (rec.type == RecordType.UPDATE || rec.type == RecordType.INSERT || rec.type == RecordType.DELETE) {
						// if undoable
						tr.undoNxtLsn = rec.lsn;
					} else {
						tr.undoNxtLsn = rec.undoNxtLsn;
					}
					// if redoable
					if (!dirtyPages.containsKey(rec.rid.pageid())) {
						dirtyPages.put(rec.rid.pageid(), rec.lsn);
					}
					break;
				case BEGIN_CHECKPOINT:
					break;
				case END_CHECKPOINT:
					// for each entry in rec.transTable and not in transTable,
					//	transTable.insert(entry.tid, entry.State, entry.lastLsn, entry.undoNxtLsn)
					for (TransactionTable.TransactionRecord trec : rec.table.records()) {
						if (!transTable.containsKey(trec.tid)) {
							transTable.insert(trec.tid, trec.lastLsn, trec.state, trec.undoNxtLsn);
						}
					}
					// for each entry in rec.dirtypages,
					//	if not in dirtyPages, dirtyPages.insert(entry.pid, entry.recLsn)
					//	else dirtyPages.get(entry.pid).recLsn = entry.recLsn
					dirtyPages.putAll(rec.dirtyPages);

					break;
				case PREPARE:
				case ABORT:
					tr = transTable.get(rec.tid);
					if (rec.type == RecordType.PREPARE) tr.state = State.PREPARED;
					else tr.state = State.UNPREPARED;
					tr.lastLsn = rec.lsn;
					break;
				case END:
					transTable.delete(rec.tid);
					break;
				default:
					throw new RuntimeException("Unrecognized record type " + rec.type);
			}
			rec = log.readLog();
		}
		
		Set<TransactionId> tidsToRemove = new HashSet<TransactionId>();
		for (TransactionRecord record : transTable.records()) {
			if (record.state == State.UNPREPARED && record.undoNxtLsn == 0) {
				System.out.println("Adding end record");
				log.logEnd(record.tid);
				tidsToRemove.add(record.tid);
			}
		}
		for (TransactionId tid : tidsToRemove) {
			transTable.delete(tid);
		}
		
		if (dirtyPages.size() != 0) {
			redoLsn = Integer.MAX_VALUE;
			for (Long val : dirtyPages.values()) {
				if (val < redoLsn)
					redoLsn = val;
			}
		}
	}
	
	// runs the redo phase of recovery
	public void restartRedo() throws DbException, IOException  {
		if (redoLsn == -1)
			return;
	
		LogRecord rec = log.readLog(redoLsn);
		
		while (rec != null) {
			Debug.println("Read " + rec, 2);
			if ((rec.type == RecordType.INSERT ||
				 rec.type == RecordType.UPDATE ||
				 rec.type == RecordType.DELETE ||
				 rec.type == RecordType.COMPENSATION)) {
				
				PageId pid = rec.rid.pageid();
				if (dirtyPages.containsKey(pid) &&
					rec.lsn >= dirtyPages.get(pid)) {
				
					DbFile dbFile = Catalog.Instance().getDbFile(pid.tableid());
					HeapPage page = null;
					if (pid.pageno() >= dbFile.numPages()) {
						try {
							byte[] data = HeapPage.createEmptyPageData(pid.tableid());
							page = new HeapPage(pid, data);
							dbFile.writePage(page);
						} catch (ParseException pe) {
							pe.printStackTrace();
							throw new DbException(pe.getMessage());
						}
					}// else {
//						page = (HeapPage)dbFile.readPage(pid);
//					}
					try {
						page = (HeapPage)BufferPool.Instance().getPage(rec.tid, pid, Permissions.READ_WRITE, false);
					} catch (TransactionAbortedException tae) {
						tae.printStackTrace();
						throw new DbException(tae.getMessage());
					}
					// latch page X
					if (page.pageLsn() < rec.lsn) {
						if (rec.type == RecordType.INSERT) {
							page.addTupleFromLog(rec.rid, LogRecord.getTuple(rec.redoData));
						} else if (rec.type == RecordType.DELETE) {
							Tuple t = LogRecord.getTuple(rec.undoData);
							t.setRecordId(rec.rid);
							page.deleteTuple(t);
						} else if (rec.type == RecordType.UPDATE) {
							for (Diff diff : rec.diffs) {
								page.updateTuple(rec.rid, diff.fieldIndex, IntField.createIntField(diff.redo));
							}
//							page.updateTuple(rec.rid, LogRecord.getTuple(rec.redoData));
						} else if (rec.type == RecordType.COMPENSATION) {
//							if (rec.undoData.length == 0) { // insert
//								page.addTupleFromLog(rec.rid, LogRecord.getTuple(rec.redoData));
//							} else if (rec.redoData.length == 0) { // delete
//								Tuple t = LogRecord.getTuple(rec.undoData);
//								t.setRecordId(rec.rid);
//								page.deleteTuple(t);
//							} else { // update
//								page.updateTuple(rec.rid, LogRecord.getTuple(rec.redoData));
//							}
							if (rec.redoData.length != 0) { // insert
								page.addTupleFromLog(rec.rid, LogRecord.getTuple(rec.redoData));
							} else if (rec.undoData.length != 0) { // delete
								Tuple t = LogRecord.getTuple(rec.undoData);
								t.setRecordId(rec.rid);
								page.deleteTuple(t);
							} else { // update
//								page.updateTuple(rec.rid, LogRecord.getTuple(rec.redoData));							
								for (Diff diff : rec.diffs) {
									page.updateTuple(rec.rid, diff.fieldIndex, IntField.createIntField(diff.redo));
								}
							}
						}
						page.setPageLsn(rec.lsn);
						page.markDirty(true);
					} else {
						dirtyPages.put(rec.rid.pageid(), page.pageLsn() + 1);
					}
					// unlatch page
				}
			}
			rec = log.readLog();
		}
	}
	
	// returns true if there is still an UNPREPARED transaction in the transaction table
	private boolean existsUnprepared() {
		for (TransactionRecord record : transTable.records()) {
			if (record.state == TransactionTable.State.UNPREPARED) {
				return true;
			}
		}
		return false;
	}
	
	// returns the maximum undoNxtLsn in the transaction table s.t. the transaction is UNPREPARED
	private long getMaxUndoNxtLsn() {
		long maxUndoLsn = 0;
		for (TransactionRecord record : transTable.records()) {
			if (record.state == TransactionTable.State.UNPREPARED &&
				record.undoNxtLsn > maxUndoLsn) {
				maxUndoLsn = record.undoNxtLsn;
			}
		}
		return maxUndoLsn;
	}
	
	// runs the undo phase of recovery
	public void restartUndo() throws DbException {
		long undoLsn;
		LogRecord rec;
		while (existsUnprepared()) {
			undoLsn = getMaxUndoNxtLsn();
			assert(undoLsn != 0);
			
			rec = log.readLog(undoLsn);
			switch (rec.type) {
				case UPDATE:
				case INSERT:
				case DELETE:
					// if undoable
					// latch handled in compensated()
					log.compensate(rec);
					// end
					transTable.get(rec.tid).undoNxtLsn = rec.prevLsn;
					if (rec.prevLsn == 0) {
						log.logEnd(rec.tid);
						transTable.delete(rec.tid);
					}
					break;
				case COMPENSATION:
					transTable.get(rec.tid).undoNxtLsn = rec.undoNxtLsn;
					break;
				case PREPARE:
				case ABORT:
					transTable.get(rec.tid).undoNxtLsn = rec.prevLsn;
					break;
				default:
					throw new RuntimeException("Unrecognized record type: " + rec.type);
			}
		}
	}
	
	public String toString() {
		StringBuffer buf = new StringBuffer();
		buf.append("Transaction Table:\n");
		buf.append(transTable.toString() + "\n");
		buf.append("Dirty Pages:\n");
		buf.append(dirtyPages.toString() + "\n");
		buf.append("Redo LSN: " + redoLsn + "\n");
		return buf.toString();
	}
	
	// runs the entire recovery algorithm
	public void recover() {
		try {
			Test test = new Test();
			ActionTimer.start(this);
			System.out.println("Restart Analysis");
			restartAnalysis();
			System.out.println("Restart Analysis took " + ActionTimer.stop(this));
//			System.out.println(this);
			BufferPool.Instance().setLogging(false);
			
			ActionTimer.start(this);
			System.out.println("Restart Redo");
			restartRedo();
			System.out.println("Restart Redo took " + ActionTimer.stop(this));			
//			System.out.println(this);
			// XXX set buffer pool dirty pages to dirtyPages
			// remove entries for non-buffer-resident pages from the buffer pool
			ActionTimer.start(this);
			restartUndo();
			BufferPool.Instance().setLogging(true);
			System.out.println("Restart undo took " + ActionTimer.stop(this));
			ActionTimer.start(this);			
			BufferPool.Instance().flush_all_pages();
			System.out.println("Flushing pages took " + ActionTimer.stop(this));
			log.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("Recovery time: " + ActionTimer.stop(ActionTimer.RECOVERY));
	}

}
