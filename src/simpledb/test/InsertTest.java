package simpledb.test;

import simpledb.*;
import java.util.*;
import java.io.*;

public class InsertTest extends Test {
  public boolean runTest(String args[]) {
    TransactionId tid = new TransactionId();
    SeqScan ss = new SeqScan(tid, file[Integer.parseInt(args[1])-1][0].id());

    Insert insOp = null;
//    try {
	  if (args[2].equals("segment")) {
		insOp = new Insert(tid, ss, segmentFile[Integer.parseInt(args[3])-1].id());
	  } else
		insOp = new Insert(tid, ss, file[Integer.parseInt(args[2])-1][1].id());
//    } catch (DbException dbe) {
//      dbe.printStackTrace();
//      System.exit(-1);
//    }

    Type typeAr[] = new Type[1];
    typeAr[0] = Type.INT_TYPE;
    TupleDesc returnTD = new TupleDesc(typeAr);

    try {
      insOp.open();
      try {
        while(true) {
	        Tuple tup = insOp.getNext();
	        if(!tup.getTupleDesc().equals(returnTD))
	          throw new DbException("wrong return type");
	        if(!args[2].equals("segment") && !tup.getField(0).toString().equals(args[3]))
	          throw new DbException("insert returned wrong number");
        }
      } catch(NoSuchElementException e) {
        insOp.close();
      }
    } catch (TransactionAbortedException te) {
      te.printStackTrace();
      return false;

    } catch (DbException e) {
      e.printStackTrace();
      return false;
    }

    // dump contents before we flush
    try {
		if (args[2].equals("segment"))
			dump(tid, segmentFile[Integer.parseInt(args[3])-1]);
		else
			dump(tid, file[Integer.parseInt(args[2])-1][1]);
    } catch (TransactionAbortedException te) {
      te.printStackTrace();
      return false;
    }

    // probably won't do much for problem set 2.
	BufferPool.Instance().prepareTransaction(tid);
    BufferPool.Instance().transactionComplete(tid);
    // XXX hack for testing purposes
    try {
      BufferPool.Instance().flush_all_pages();
    } catch (Exception e) {
      e.printStackTrace();
    }

    return true;
  }
}
