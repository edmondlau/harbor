//
//  TransactionUpdateTest.java
//  
//
//  Created by Edmond Lau on 3/6/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.test;

import simpledb.*;
import java.util.*;
import java.io.*;

/**
 * Tests running concurrent transactions.
 * You do not need to pass this test for ps2.
 */
public class TransactionUpdateTest extends Test {
  private int NTHREADS = 50;
  
  private Random rand = new Random();

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

	  UpdateFunction func = new UpdateFunction() {
		public Tuple update (Tuple t) {
			IntField intf = (IntField)t.getField(0);
			int i = Integer.parseInt(intf.toString());
			Tuple tup = new Tuple(t.getTupleDesc());
			tup.setField(0, IntField.createIntField(i+1));
			return tup;
		}
	  };

      while(true) {
		TransactionId tid = new TransactionId();		  
		try {
						
			SeqScan ss = new SeqScan(tid, file[0][0].id());

			Update update = new Update(tid, ss, func);
			Utilities.execute(update);
		  
		  if (rand.nextBoolean()) {
			Debug.println("Random abort");
			BufferPool.Instance().abortTransaction(tid);
			throw new TransactionAbortedException();
		  }
		  
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