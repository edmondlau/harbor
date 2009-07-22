//
//  LogRecord.java
//  
//
//  Created by Edmond Lau on 2/6/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.log;

import simpledb.*;
import java.io.*;
import java.util.*;

/**
 * A LogRecord is an abstraction for a record of a part of a transaction.
 * It can write itself out to a DataOutputStream or read itself back from a
 * RandomAcessFile.
 *
 * Implementation notes:
 * - An INSERT has an empty undoData field but a filled redoData field, and an empty diffs list.
 * - A DELETE has a filled undoData field but an empty redoData field, and an empty diffs list.
 * - An UPDATE has empty undoData and redoData, and a diffs list
 * - A COMPENSATION log record has the opposite structure of the corresponding fields above.
 */

public class LogRecord {

	public enum RecordType { UPDATE, INSERT, DELETE, COMPENSATION, PREPARE, PREPARE_TO_COMMIT, ABORT, END, BEGIN_CHECKPOINT, END_CHECKPOINT };
	public static final RecordType[] RecordTypeValues = RecordType.values();
	
	public static final int[] EMPTY_DATA = new int[0];
	
	public static final List<Diff> EMPTY_DIFFS = new ArrayList<Diff>();
	
	public static final RecordId EMPTY_RID = new RecordId(-1, -1, -1);

	public long lsn;
	public RecordType type;
	public TransactionId tid;
	public RecordId rid;
	
	public long prevLsn;
	public long undoNxtLsn;
	
	public int[] undoData;
	public int[] redoData;
	
	public List<Diff> diffs;
	
	public TransactionTable table;
	public Map<PageId, Long> dirtyPages;
	
	// An update log record 
	
	private LogRecord() {
	}
	
	public LogRecord(long lsn, RecordType type, TransactionId tid, RecordId rid, long prevLsn, long undoNxtLsn,
		int[] undoData, int[] redoData) {
		this.lsn = lsn;
		this.type = type;
		this.tid = tid;
		this.rid = rid;
		this.prevLsn = prevLsn;
		this.undoNxtLsn = undoNxtLsn;
		this.undoData = undoData;
		this.redoData = redoData;
		this.diffs = EMPTY_DIFFS;
	}
	
	public LogRecord(long lsn, RecordType type, TransactionId tid, long prevLsn, long undoNxtLsn) {
		this(lsn, type, tid, EMPTY_RID, prevLsn, undoNxtLsn, EMPTY_DATA, EMPTY_DATA);
	}
	
	// factory methods for all the different log record types
	public static LogRecord createUpdateRecord(long lsn, TransactionId tid, RecordId rid, long prevLsn, Tuple undo, Tuple redo) {
		LogRecord rec = new LogRecord(lsn, RecordType.UPDATE, tid, prevLsn, 0);
		rec.rid = rid;
		rec.diffs = Diff.createDiffs(undo, redo);
		return rec;
	}	
	
	public static LogRecord createInsertRecord(long lsn, TransactionId tid, RecordId rid, long prevLsn, Tuple t) {
		return new LogRecord(lsn, RecordType.INSERT, tid, rid, prevLsn, 0, EMPTY_DATA, getData(t));
	}
	
	public static LogRecord createDeleteRecord(long lsn, TransactionId tid, RecordId rid, long prevLsn, Tuple t) {
		return new LogRecord(lsn, RecordType.DELETE, tid, rid, prevLsn, 0, getData(t), EMPTY_DATA);
	}
	
	public static LogRecord createCLR(long lsn, TransactionId tid, RecordId rid, long prevLsn, long undoNxtLsn, int[] undo, int[] redo, List<Diff> diffs) {
		LogRecord rec = new LogRecord(lsn, RecordType.COMPENSATION, tid, rid, prevLsn, undoNxtLsn, undo, redo);
		rec.diffs = diffs;
		return rec;
	}

	public static LogRecord createPrepareRecord(long lsn, TransactionId tid, long prevLsn) {
		return new LogRecord(lsn, RecordType.PREPARE, tid, prevLsn, 0);
	}
	
	public static LogRecord createPrepareToCommitRecord(long lsn, TransactionId tid, long prevLsn) {
		return new LogRecord(lsn, RecordType.PREPARE, tid, prevLsn, 0);
	}	
	
	public static LogRecord createAbortRecord(long lsn, TransactionId tid, long prevLsn) {
		return new LogRecord(lsn, RecordType.ABORT, tid, prevLsn, 0);
	}
	
	public static LogRecord createEndRecord(long lsn, TransactionId tid, long prevLsn) {
		return new LogRecord(lsn, RecordType.END, tid, prevLsn, 0);	
	}
	
	public static LogRecord createBeginCheckpointRecord(long lsn) {
		return new LogRecord(lsn, RecordType.BEGIN_CHECKPOINT, new TransactionId(0), 0, 0);
	}
	
	public static LogRecord createEndCheckpointRecord(long lsn, TransactionTable table, Map<PageId, Long> dirtyPages) {
		LogRecord rec = new LogRecord(lsn, RecordType.END_CHECKPOINT, null, 0, 0);
		rec.table = table;
		rec.dirtyPages = dirtyPages;
		return rec;
	}
	
	// converts tuple => int[]
	private static int[] getData(Tuple t) {
		int numFields = t.getTupleDesc().numFields();
		int[] data = new int[numFields];
		for (int i = 0; i < numFields; i++) {
			data[i] = ((IntField)t.getField(i)).val();
		}
		return data;
	}
	
	// converts int[] => tuple
	public static Tuple getTuple(int[] data) {
		TupleDesc td = Utilities.createTupleDesc(data.length);
		Tuple t = new Tuple(td);
		for (int i = 0; i < data.length; i++) {
			t.setField(i, IntField.createIntField(data[i]));
		}
		return t;
	}
	
	// @returns a RecordId read from current location in file
	private static RecordId readRid(RandomAccessFile file) throws IOException {
		int tableid = file.readInt();
		int pageno = file.readInt();
		int tupleno = file.readInt();
		return new RecordId(tableid, pageno, tupleno);
	}
	
	// writes the record id to the output stream
	private void writeRid(DataOutputStream dos, RecordId rid) throws IOException {
		dos.writeInt(rid.tableid());
		dos.writeInt(rid.pageno());
		dos.writeInt(rid.tupleno());
	}
	
	// @returns a serialized int[] read from current location in file
	private static int[] readData(RandomAccessFile file) throws IOException {
		int nFields = file.readInt();
		int[] data = new int[nFields];
		for (int i = 0; i < nFields; i++) {
			data[i] = file.readInt();
		}
		return data;
	}
	
	// serializes the int[] to the output stream
	private void writeData(DataOutputStream dos, int[] data) throws IOException {
		dos.writeInt(data.length);
		for (int i = 0; i < data.length; i++) {
			dos.writeInt(data[i]);
		}
	}
	
	private void writeDiffs(DataOutputStream dos, List<Diff> diffList) throws IOException {
		dos.writeInt(diffList.size());
		for (Diff diff : diffList) {
			dos.writeInt(diff.fieldIndex);
			dos.writeInt(diff.undo);
			dos.writeInt(diff.redo);
		}
	}
	
	private static List<Diff> readDiffs(RandomAccessFile file) throws IOException {
		int size = file.readInt();
		LinkedList<Diff> diffs = new LinkedList<Diff>();
		for (int i = 0; i < size; i++) {
			int fieldIndex = file.readInt();
			int undo = file.readInt();
			int redo = file.readInt();
			diffs.add(new Diff(fieldIndex, undo, redo));
		}
		return diffs;
	}
	
	// @returns a LogRecord read from the current location in the file.
	public static LogRecord readRecord(RandomAccessFile file) throws IOException {
		LogRecord rec = new LogRecord();
		rec.lsn = file.getFilePointer();
		rec.type = RecordTypeValues[file.readInt()];
		
		switch (rec.type) {
			case UPDATE:
			case INSERT:
			case DELETE:
			case COMPENSATION:	
				rec.tid = new TransactionId(file.readInt());
				rec.rid = readRid(file);
				rec.prevLsn = file.readLong();
				rec.undoNxtLsn = file.readLong();
				rec.undoData = readData(file);
				rec.redoData = readData(file);
				rec.diffs = readDiffs(file);
				break;
			case PREPARE:
			case ABORT:
			case END:
			case BEGIN_CHECKPOINT:
				rec.tid = new TransactionId(file.readInt());
				rec.prevLsn = file.readLong();
				rec.undoNxtLsn = file.readLong();
				break;
			case END_CHECKPOINT:
				rec.table = new TransactionTable();
				rec.dirtyPages = new HashMap<PageId, Long>();
			
				int nRecs = file.readInt();
				for (int i = 0; i < nRecs; i++) {
					TransactionId tid = new TransactionId(file.readInt());
					TransactionTable.State state = TransactionTable.StateValues[file.readInt()];
					long lastLsn = file.readLong();
					long undoNxtLsn = file.readLong();
					rec.table.insert(tid, lastLsn, state, undoNxtLsn);
				}
				
				int nPages = file.readInt();
				for (int i = 0; i < nPages; i++) {
					int tableid = file.readInt();
					int pageno = file.readInt();
					PageId pid = new PageId(tableid, pageno);
					long lsn = file.readLong();
					rec.dirtyPages.put(pid, lsn);
				}
				break;
			default:
				throw new RuntimeException("Unrecognized record type: " + rec.type);
		}

		return rec;
	}
	
	// @returns the byte[] representation of this LogRecord
	public byte[] getBytes() throws IOException {
	    ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);
		// write out all the data
		dos.writeInt(type.ordinal());
		
		switch (type) {
			case UPDATE:
			case INSERT:
			case DELETE:
			case COMPENSATION:
				dos.writeInt(tid.value());
				writeRid(dos, rid);
				dos.writeLong(prevLsn);
				dos.writeLong(undoNxtLsn);
				writeData(dos, undoData);
				writeData(dos, redoData);
				writeDiffs(dos, diffs);
				break;
			case PREPARE:
			case ABORT:
			case END:
			case BEGIN_CHECKPOINT:
				dos.writeInt(tid.value());
				dos.writeLong(prevLsn);
				dos.writeLong(undoNxtLsn);
				break;
			case END_CHECKPOINT:
				dos.writeInt(table.records().size());
				for (TransactionTable.TransactionRecord rec : table.records()) {
					dos.writeInt(rec.tid.value());
					dos.writeInt(rec.state.ordinal());
					dos.writeLong(rec.lastLsn);
					dos.writeLong(rec.undoNxtLsn);
				}

				dos.writeInt(dirtyPages.size());
				for (PageId pid : dirtyPages.keySet()) {
					dos.writeInt(pid.tableid());
					dos.writeInt(pid.pageno());
					dos.writeLong(dirtyPages.get(pid));
				}
				break;
			default:
				throw new RuntimeException("Unrecognized record type: " + type);
		}
		
		try {
		  dos.flush();
		} catch(IOException e) {
		  e.printStackTrace();
		}
		return baos.toByteArray();
	}
	
	public String toString() {
		StringBuffer buf = new StringBuffer();
		buf.append(lsn + " ");
		buf.append(type + " ");
		
		switch (type) {
			case UPDATE:
			case INSERT:
			case DELETE:
			case COMPENSATION:
				buf.append("T" + tid + " ");
				buf.append(rid + " ");
				buf.append("PREV_" + prevLsn + " ");
				buf.append("UNDO_NXT_" + undoNxtLsn + " ");
				buf.append(Arrays.toString(undoData) + " ");
				buf.append(Arrays.toString(redoData) + " ");
				buf.append(diffs);
				break;
			case PREPARE:
			case ABORT:
			case END:
			case BEGIN_CHECKPOINT:
				buf.append("T" + tid + " ");
				buf.append("PREV_" + prevLsn + " ");
				buf.append("UNDO_NXT_" + undoNxtLsn + " ");
				break;
			case END_CHECKPOINT:
				buf.append("\ntable:\n" + table.toString() + " ");
				buf.append("\ndirty pages:\n" + dirtyPages.toString() + " ");
				break;
			default:
				throw new RuntimeException("Unrecognized record type: " + type);				
		}
		
		return buf.toString();
	}
}

class Diff {
	public int fieldIndex;
	public int undo;
	public int redo;
	
	public Diff(int fieldIndex, int undo, int redo) {
		this.fieldIndex = fieldIndex;
		this.undo = undo;
		this.redo = redo;
	}
	
	public static List<Diff> createDiffs(Tuple oldTuple, Tuple newTuple) {
		List<Diff> diffs = new LinkedList<Diff>();
		for (int i = 0; i < oldTuple.getTupleDesc().numFields(); i++) {
			IntField intf1 = (IntField)oldTuple.getField(i);
			IntField intf2 = (IntField)newTuple.getField(i);
			if (!intf1.equals(intf2)) {
				diffs.add(new Diff(i, intf1.val(), intf2.val()));
			}
		}
		return diffs;
	}
	
	public static List<Diff> reverse(List<Diff> diffs) {
		LinkedList<Diff> newDiffs = new LinkedList();
		for (Diff diff : diffs) {
			int temp = diff.undo;
			diff.undo = diff.redo;
			diff.redo = temp;
			newDiffs.add(diff);
		}
		return newDiffs;
	}
	
	public String toString() {
		return "<" + fieldIndex + ":" + undo + "->" + redo + ">";
	}
}
