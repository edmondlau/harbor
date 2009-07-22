package simpledb.test;

import java.util.*;
import java.io.*;
import simpledb.*;

public class JoinTest extends Test {
  public boolean runTest(String args[]) {
    TransactionId tid = new TransactionId();
    SeqScan ss1 = new SeqScan(tid, file[Integer.parseInt(args[1])-1][0].id());
    SeqScan ss2 = new SeqScan(tid, file[Integer.parseInt(args[2])-1][1].id());
    JoinPredicate p = new JoinPredicate(Integer.parseInt(args[3]),
                                        Integer.parseInt(args[4]),
                                        Integer.parseInt(args[5]));
    Join j = new Join(p, ss1, ss2);

    // print out the joined table
    try {
      ss1.open();
      ss2.open();

      try {
        while (true) {
          Tuple tup = j.getNext();
          tup.print();
        }
      } catch(NoSuchElementException e) {
        ss1.close();
        ss2.close();
      }
    } catch (TransactionAbortedException te) {
      te.printStackTrace();
      return false;

    } catch (DbException e) {
      e.printStackTrace();
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
