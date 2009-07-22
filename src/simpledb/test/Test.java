package simpledb.test;

import simpledb.*;
import simpledb.version.*;
import java.util.*;
import java.io.*;

public class Test {
  DbFile file[][];
  DbFile segmentFile[];
  final static int MAX_COLUMNS = 128;
  final static int MAX_TABLES = 50;
  final static int MAX_SEGMENTS = 200;
  final static int NUM_COLUMNS = 16;

  /*
   * Create all tables of up to 128 columns.
   * The table with 1 column is in a file called "f0.dat"
   * The table with 2 columns is in a file called "f1.dat"
   * ...
   */
  public Test() {
    file = new HeapFile[MAX_COLUMNS][MAX_TABLES];

	ActionTimer.start(this);
    for(int i=0; i<MAX_COLUMNS; i++) {
      Type ta[] = new Type[i+1];
      for(int j=0; j<i+1; j++)
        ta[j] = Type.INT_TYPE;
      TupleDesc t = new TupleDesc(ta);
      for(int k=0; k<MAX_TABLES; k++) {
        file[i][k] = new HeapFile(new File("f" + i + "." + k + ".dat"));
        Catalog.Instance().addTable(file[i][k], t);
      }
    }
	
	segmentFile = new SegmentedHeapFile[MAX_TABLES];
	
	for (int i = 0; i < MAX_TABLES; i++) {
		Type ta[] = new Type[NUM_COLUMNS];
		for(int j=0; j<NUM_COLUMNS; j++)
			ta[j] = Type.INT_TYPE;
		TupleDesc t = new TupleDesc(ta);
		segmentFile[i] = new SegmentedHeapFile(new File("sf" + (NUM_COLUMNS-1) + "." + i + ".dat"));
		Catalog.Instance().addTable(segmentFile[i], t);
	}
	
	Debug.println("Initializing test catalog: " + ActionTimer.stop(this));
  }

  public void dump(TransactionId tid, DbIterator dbi) throws TransactionAbortedException {
    try {
      dbi.open();
    } catch (DbException e) {
    }

    try {
      while (true) {
        Tuple tup = dbi.getNext();
        tup.print();
      }
    } catch(NoSuchElementException e) {
      dbi.close();
    }
  }
  
  public void dump(DbFile file, int low, int high, int deleteLow) throws TransactionAbortedException {
	TransactionId tid = new TransactionId();	
	IndexedSeqScan ss = new IndexedSeqScan(tid, file.id());
	ss.restrictInsertionTime(new Epoch(low), new Epoch(high));
	ss.restrictDeletionTime(new Epoch(deleteLow));
	dump(tid, ss);
  }

  public void dump(TransactionId tid, DbFile file) throws TransactionAbortedException {
	if (file instanceof SegmentedHeapFile) {
		IndexedSeqScan ss = new IndexedSeqScan(tid, file.id(), true);
		dump(tid, ss);
	} else {
		SeqScan ss = new SeqScan(tid, file.id());
		dump(tid, ss);
	}
  }

  public void dump(DbFile file) throws TransactionAbortedException {
    TransactionId tid = new TransactionId();
	dump(tid, file);
    BufferPool.Instance().transactionComplete(tid);
  }

  public boolean runTest(String args[]) {
    return false;
  }
}
