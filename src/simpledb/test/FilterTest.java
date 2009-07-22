package simpledb.test;

import simpledb.*;
import java.util.*;
import java.io.*;

public class FilterTest extends Test {

  public boolean runTest(String args[]) {
    TransactionId tid = new TransactionId();
    SeqScan ss = new SeqScan(tid, file[Integer.parseInt(args[1])-1][0].id());
    Predicate p = new Predicate(Integer.parseInt(args[2]),
                                Integer.parseInt(args[3]),
								IntField.createIntField(new Integer(args[4])));
    Filter f = new Filter(p, ss);

    // dump results of filter
    try {
      dump(tid, f);
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
