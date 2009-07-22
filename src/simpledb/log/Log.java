//
//  Log.java
//  
//
//  Created by Edmond Lau on 2/6/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.log;

import simpledb.*;
import simpledb.version.*;
import simpledb.log.LogRecord.*;
import java.io.*;
import java.util.*;

/**
 * The Log singleton provides an interface to the on-disk recovery log.
 * It supports writing to the tail of the log as well as reading LogRecords
 * at arbitrary locations in the log via seeking.  The Log also provides
 * logic to rollback transactions by undoing updates and logging compensation
 * records for the aborts.
 *
 * An LSN (log sequence number) refers to the address of the on-disk log record.
 * Log garbage collection has not been implemented.
 */
public class Log {

	public static final String LOG_DIR = "/data" + File.separator + "edmond" + File.separator;
	public static final String LOG_FILE = "log";
//	public static final String LOG_FILE = "log";
	private static Log instance;
	public static final long LOG_HEADER_ADDR = 4;

	public static boolean SINGLE_DISK = false;

	// current log file
	private RandomAccessFile file;
	
	private long unflushedBytes;
	
	public Log(String logFile) {
		try {
			// create the log file if it doesn't exist
			File log = new File(logFile);
			if (!log.exists()) {
				log.createNewFile();
			}
			file = new RandomAccessFile(logFile, "rw");
			file.writeInt(-1);
		} catch (Exception ioe) {
			ioe.printStackTrace();
		}
	}
	
	public static Log Instance() {
		if (instance == null) {
			if (!SINGLE_DISK && (new File(LOG_DIR)).exists())
				instance = new Log(LOG_DIR + LOG_FILE);
			else 
				instance = new Log(LOG_FILE);
		}
		return instance;
	}
	
	public static void setSingleDisk() {
		SINGLE_DISK = true;
	}
	
	// closes the log file
	public synchronized void close() {
		try {
			file.close();
		} catch (Exception ioe) {
			ioe.printStackTrace();
		}
	}

	// reads the next log record from the log from the current log pointer
	public synchronized LogRecord readLog() {
		try {
			LogRecord rec = LogRecord.readRecord(file);
			return rec;
		} catch (IOException ioe) {
//			ioe.printStackTrace();
			return null;
		}
	}
	
	public synchronized long endOfLog() {
		try {
			return file.length();
		} catch (IOException ioe) {
			ioe.printStackTrace();
			throw new RuntimeException("Can't read end of log");
		}
	}
	
	// reads the log record with the specified LSN
	public synchronized LogRecord readLog(long lsn) {
		try {
			file.seek(lsn);
		} catch (IOException ioe) {
			System.out.println("LSN " + lsn + " not found");
			ioe.printStackTrace();
		}
		return readLog();
	}
	
	// returns the previous lsn stored for tid in the TransactionTable
	private long getPrevLsn(TransactionId tid) {
		long prevLsn = 0;
		if (TransactionTable.Instance().containsKey(tid)) {
			prevLsn = TransactionTable.Instance().get(tid).lastLsn;
		}
		return prevLsn;
	}
	
	// writes the specified record at the current log pointer
	private synchronized void logWrite(LogRecord rec) {
		try {
			if (file.getFilePointer() != file.length()) {
				file.seek(file.length());
			}
			long oldLength = file.length();
			file.write(rec.getBytes());
			long newLength = file.length();
			unflushedBytes += (newLength - oldLength);
		} catch (Exception ioe) {
			ioe.printStackTrace();
		}
	}
	
	public long unflushedLength() {
		return unflushedBytes;
	}
	
	// logs a record of the specified type
	private synchronized long logRecord(RecordType type, TransactionId tid, RecordId rid, Tuple oldTuple, Tuple newTuple) {
		try {
			long lsn = file.getFilePointer();
			long prevLsn = getPrevLsn(tid);
			LogRecord rec;
			
			switch (type) {
				case UPDATE:
					rec = LogRecord.createUpdateRecord(lsn, tid, rid, prevLsn, oldTuple, newTuple);
					break;
				case INSERT:
					rec = LogRecord.createInsertRecord(lsn, tid, rid, prevLsn, newTuple);
					break;
				case DELETE:
					rec = LogRecord.createDeleteRecord(lsn, tid, rid, prevLsn, oldTuple);
					break;		
				case ABORT:
					rec = LogRecord.createAbortRecord(lsn, tid, prevLsn);
					break;
				case PREPARE:
					rec = LogRecord.createPrepareRecord(lsn, tid, prevLsn);
					break;
				case PREPARE_TO_COMMIT:
					rec = LogRecord.createPrepareToCommitRecord(lsn, tid, prevLsn);
					break;					
				case END:
					rec = LogRecord.createEndRecord(lsn, tid, prevLsn);
					break;
				default:
					throw new RuntimeException("Unrecognized record type" + type);
			}
			logWrite(rec);

			if (type == RecordType.UPDATE || type == RecordType.INSERT || type == RecordType.DELETE) {
				TransactionTable.Instance().update(tid, lsn, TransactionTable.State.UNPREPARED, lsn);
			} else if (type == RecordType.PREPARE) {
				TransactionTable.Instance().update(tid, lsn, TransactionTable.State.PREPARED, lsn);
			} else if (type == RecordType.END) {
				if (TransactionTable.Instance().containsKey(tid)) { 
					TransactionTable.Instance().delete(tid);
				}
			} else {
				TransactionTable.State state;
				if (TransactionTable.Instance().containsKey(tid)) {
					state = TransactionTable.Instance().get(tid).state;
				} else {
					state = TransactionTable.State.UNPREPARED;
				}
				TransactionTable.Instance().update(tid, lsn, state, lsn);
			}
			
			return lsn;			
		} catch (IOException ioe) {
			ioe.printStackTrace();
			throw new RuntimeException(ioe.getMessage());
		}
	}
	
	
	// logUpdate, logInsert, and logCLR are called within the HeapFile
	public long logUpdate(TransactionId tid, RecordId rid, Tuple oldTuple, Tuple newTuple) {
		return logRecord(RecordType.UPDATE, tid, rid, oldTuple, newTuple);
	}
	
	public long logInsert(TransactionId tid, RecordId rid, Tuple t) {
		return logRecord(RecordType.INSERT, tid, rid, null, t);
	}
	
	public long logDelete(TransactionId tid, RecordId rid, Tuple t) {
		return logRecord(RecordType.DELETE, tid, rid, t, null);
	}
	
	public long logCLR(TransactionId tid, RecordId rid, Tuple oldTuple, Tuple newTuple) {
		return logRecord(RecordType.COMPENSATION, tid, rid, oldTuple, newTuple);
	}
	
	public long logAbort(TransactionId tid) {
		return logRecord(RecordType.ABORT, tid, null, null, null);
	}
	
	public long logPrepare(TransactionId tid) {
		return logRecord(RecordType.PREPARE, tid, null, null, null);
	}
	
	public long logPrepareToCommit(TransactionId tid) {
		return logRecord(RecordType.PREPARE_TO_COMMIT, tid, null, null, null);
	}
	
	// an end record is logged at 1) transaction commit or 2) after an abort
	public long logEnd(TransactionId tid) {
		return logRecord(RecordType.END, tid, null, null, null);
	}
	
	public void logCheckpoint() {
		try {
			long lsn;
			LogRecord rec;
			synchronized (this) {
				lsn = file.getFilePointer();
				rec = LogRecord.createBeginCheckpointRecord(lsn);
				logWrite(rec);
			}
			
			Checkpoint.ARIES_CHKPT.write(lsn);
			
			Map<PageId, Long> dirtyPages = BufferPool.Instance().getDirtyPages();
			
			synchronized (this) {
				lsn = file.getFilePointer();
				rec = LogRecord.createEndCheckpointRecord(lsn, TransactionTable.Instance(), dirtyPages);
				logWrite(rec);				
			}
			
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
			throw new RuntimeException(ioe.getMessage());
		}
	}
	
	// undoes changes for the specified rec and logs a compensation record
	void compensate(LogRecord rec) throws DbException {
		try {
		
			// undo the update
			PageId pid = rec.rid.pageid();
			
//			LatchManager.Instance().latch(pid, Permissions.EXCLUSIVE);
			//DbFile dbFile = Catalog.Instance().getDbFile(pid.tableid());
			HeapPage page = (HeapPage)BufferPool.Instance().getPage(rec.tid, pid, Permissions.READ_WRITE); //(HeapPage)dbFile.readPage(pid);
			if (rec.type == RecordType.UPDATE) {
//				page.updateTuple(rec.rid, LogRecord.getTuple(rec.undoData));
				for (Diff diff : rec.diffs) {
					page.updateTuple(rec.rid, diff.fieldIndex, IntField.createIntField(diff.undo));
				}
			} else if (rec.type == RecordType.INSERT) {
				Tuple insertedTuple = LogRecord.getTuple(rec.redoData);
				insertedTuple.setRecordId(rec.rid);
				page.deleteTuple(insertedTuple);
			} else if (rec.type == RecordType.DELETE) {
				Tuple deletedTuple = LogRecord.getTuple(rec.undoData);
				page.addTupleFromLog(rec.rid, deletedTuple);
			} else {
				throw new RuntimeException("Unexpected record type " + rec.type);
			}
			synchronized(this) {
			
				// log the clr record
				long lsn = file.getFilePointer();
				long prevLsn = getPrevLsn(rec.tid);
				// swap the undo and redo data
//				LogRecord clr = LogRecord.createCLR(lsn, rec.tid, rec.rid, TransactionTable.Instance().get(rec.tid).lastLsn, rec.prevLsn,
//					LogRecord.getTuple(rec.redoData), LogRecord.getTuple(rec.undoData));
				LogRecord clr = LogRecord.createCLR(lsn, rec.tid, rec.rid, TransactionTable.Instance().get(rec.tid).lastLsn, rec.prevLsn,
					rec.redoData, rec.undoData, Diff.reverse(rec.diffs));
				logWrite(clr);
					
				page.setPageLsn(rec.lsn);
				TransactionTable.Instance().get(rec.tid).lastLsn = rec.lsn;
			
			}
//			LatchManager.Instance().unlatch(pid);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DbException(e.getMessage());
		}
	}
	
	// rollback operates on the page-level, thereby bypassing the normal logging
	// that happens during normal processing in HeapFile
	public void rollback(TransactionId tid) throws DbException {
		try {
			long undoNxt = TransactionTable.Instance().get(tid).undoNxtLsn;
			// in aries, there is a concept of partial rollback to a save point
			// we have no save point, so the saveLsn is set to 0
			long saveLsn = 0;
			
			while (saveLsn < undoNxt) {
				LogRecord rec = readLog(undoNxt);
				switch (rec.type) {
					case UPDATE:
					case INSERT:
					case DELETE:
						compensate(rec);
						undoNxt = rec.prevLsn;	
						break;
					case COMPENSATION:
						undoNxt = rec.undoNxtLsn;
						break;
					default:
						undoNxt = rec.prevLsn;
				}
				TransactionTable.Instance().get(tid).undoNxtLsn = undoNxt;
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new DbException(e.getMessage());
		}
	}
	
	// flushes the log to disk
	public void flushLog() {
		try {
			file.getFD().sync();
			unflushedBytes = 0;
		} catch (Exception ioe) {
			ioe.printStackTrace();
		}
	}
}
