package simpledb.test;

import simpledb.*;
import java.util.*;
import java.io.*;

/**
 * Tests running concurrent transactions.
 * You do not need to pass this test for ps2.
 */
public class TransactionTest extends Test {
  private int NTHREADS = 50;

  public boolean runTest(String args[]) {
    NTHREADS = Integer.parseInt(args[1]);

    Thread[] list = new Thread[NTHREADS];
    for(int i=0; i<NTHREADS; i++) {
      list[i] = new Thread(new XactionTester(i));
      list[i].start();
    }

    boolean waiting = true;
    while(waiting) {
      waiting = false;
      for(int i=0; i<NTHREADS; i++)
        if(list[i].isAlive())
          waiting = true;
    }

    // XXX hack for testing purposes
    try {
      BufferPool.Instance().flush_all_pages();
    } catch (Exception e) {
      e.printStackTrace();
    }

    try {
      dump(file[0][0]);
    } catch (TransactionAbortedException te) {
      te.printStackTrace();
      return false;
    }
    return true;
  }

  class XactionTester implements Runnable {
    int id;
    final Random random = new Random();

    public XactionTester(int id) {
      this.id = id;
    }

    public void run() {

      while(true) {
        TransactionId tid = new TransactionId();				
        SeqScan ss = new SeqScan(tid, file[0][0].id());

        Type typeAr[] = new Type[1];
        typeAr[0] = Type.INT_TYPE;
        TupleDesc td = new TupleDesc(typeAr);

        // read the value out of the table
        try {
          ss.open();
          Tuple tup = ss.getNext();
          ss.close();
          IntField intf = (IntField) tup.getField(0);
          int i = Integer.parseInt(intf.toString());

		  Debug.println("read : " + i);

          // create a TupleIterator so that Insert can insert this new value
          // into the table.
          Tuple t = new Tuple(td);          
		  t.setField(0, IntField.createIntField(new Integer(i+1)));
          // sleep thread
          try {
            Thread.currentThread().sleep(NTHREADS + random.nextInt(NTHREADS));
          } catch (InterruptedException e) {
          }

          // delete old values (i.e., just one row) from table
          Delete delOp = new Delete(tid, ss);
          delOp.open();
          delOp.getNext();
		  
		  Debug.println("deleted : " + i);

          // set up a Set with a tuple that is one higher than the old one.
          LinkedList hs = new LinkedList();
          hs.add(t);
          TupleIterator ti = new TupleIterator(td, hs);

          // sleep thread
          try {
            Thread.currentThread().sleep(NTHREADS + random.nextInt(NTHREADS));
          } catch (InterruptedException e) {
          }

          // insert this new tuple into the table
          Insert insOp = new Insert(tid, ti, file[0][0].id());
          insOp.open();
          insOp.getNext();
		  
		  Debug.println("wrote : " + t.getField(0));
		  
		  BufferPool.Instance().prepareTransaction(tid);
          BufferPool.Instance().transactionComplete(tid);
          break;

        } catch (DbException dbe) {
          dbe.printStackTrace();
          break;

        } catch (TransactionAbortedException te) {
          // System.out.println("thread " + id + " killed");
          // give someone else a chance
          try {
            Thread.currentThread().sleep(NTHREADS * 100 + random.nextInt(NTHREADS * 20));
          } catch (InterruptedException e) {
          }

          continue;
        }
      }
      // System.out.println("thread " + id + " done");
    }
  }
}