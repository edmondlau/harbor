//
//  DumpTest.java
//  
//
//  Created by Edmond Lau on 1/26/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.test;

import simpledb.*;
import java.util.*;
import java.io.*;
import simpledb.version.*;

public class DumpTest extends Test {
  public boolean runTest(String args[]) {
    try {
		BufferPool.Instance().setLogging(false);
//		DbFile file = new HeapFile(new File(args[1]));
		DbFile file = Catalog.Instance().getDbFile(args[1]);
		if (args.length >= 3) {
			int low = Integer.parseInt(args[2]);
			TransactionId tid = new TransactionId();	
			IndexedSeqScan ss = new IndexedSeqScan(tid, file.id(), true);
			ss.restrictInsertionTime(new Epoch(low), new Epoch(Integer.MAX_VALUE));
			ss.restrictDeletionTime(new Epoch(IndexedSeqScan.NO_DELETE_RESTRICTION));
			Predicate pred = new Predicate(0, Predicate.GREATER_THAN_OR_EQ, IntField.createIntField(low));
			Filter filter = new Filter(pred, ss);
			dump(tid, filter);
		} else {
			dump(file);
		}
    } catch (TransactionAbortedException te) {
      te.printStackTrace();
      return false;
    }
    return true;
  }
}
