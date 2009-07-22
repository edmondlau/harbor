package simpledb.test;

import java.util.*;
import simpledb.*;
import java.io.*;
import simpledb.*;

public class DeleteTest extends Test {

  public boolean runTest(String args[]) {
    TransactionId tid = new TransactionId();				
    SeqScan ss = new SeqScan(tid, file[Integer.parseInt(args[1])-1][0].id());
    Predicate p = new Predicate(Integer.parseInt(args[2]),
                                Integer.parseInt(args[3]),
                                IntField.createIntField(new Integer(args[4])));
    Filter f = new Filter(p, ss);
    Delete delOp = new Delete(tid, f);

    Type typeAr[] = new Type[1];
    typeAr[0] = Type.INT_TYPE;
    TupleDesc returnTD = new TupleDesc(typeAr);

    try {
      delOp.open();

      try {
        while (true) {
          Tuple tup = delOp.getNext();
          if(!tup.getTupleDesc().equals(returnTD))
            throw new DbException("wrong return type");
          if(!tup.getField(0).toString().equals(args[5]))
            throw new DbException("insert returned wrong number");
        }
      } catch(NoSuchElementException e) {
        delOp.close();
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
      dump(tid, file[Integer.parseInt(args[1])-1][0]);
    } catch (TransactionAbortedException te) {
      te.printStackTrace();
      return false;
    }

    // probably won't do much for problem set 2.
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
