package simpledb.test;

import simpledb.*;
import java.util.*;
import java.io.*;

// this is the test from ps2.html
public class DemoTest extends Test {
  public boolean runTest(String args[]) {
    // construct a 3-column table schema
    Type typeAr[] = new Type[3];
    typeAr[0] = Type.INT_TYPE;
    typeAr[1] = Type.INT_TYPE;
    typeAr[2] = Type.INT_TYPE;
    TupleDesc t = new TupleDesc(typeAr);

    // create the table, associate it with some_data_file.dat
    // and tell the catalog about the schema of this table.
    HeapFile table1 = new HeapFile(new File("some_data_file.dat"));
    Catalog.Instance().addTable(table1, t);
    
    // construct the query: SeqScan spoonfeeds tuples to Filter, which uses
    // a predicate that filters tuples based on the first column: only fields
    // greater than 5 are passed up.
    TransactionId tid = new TransactionId();                                
    SeqScan ss = new SeqScan(tid, table1.id());
    Predicate p = new Predicate(0, Predicate.GREATER_THAN, IntField.createIntField(new Integer(5)));
    Filter f = new Filter(p, ss);

    // and run it
    try {
      f.open();

      try {
        while (true) {
          Tuple tup = f.getNext();
          tup.print();
        }
      } catch(NoSuchElementException e) {
        f.close();
      }
    } catch (TransactionAbortedException e) {
      e.printStackTrace();

    } catch (DbException e) {
      e.printStackTrace();
      return false;
    }

    BufferPool.Instance().transactionComplete(tid);
    return true;
  }
}
