package simpledb.test;

import simpledb.*;
import java.util.*;
import java.io.*;

public class AggregateTest extends Test {

  public boolean runTest(String args[]) {
    TransactionId tid = new TransactionId();
    SeqScan ss = new SeqScan(tid, file[Integer.parseInt(args[1])-1][0].id());
    Aggregator aggregator = new IntAggregator(Integer.parseInt(args[2]));
    Aggregate ag = new Aggregate(ss, Integer.parseInt(args[3]), Integer.parseInt(args[4]), aggregator);

    try {
      ag.open();

      while(true) {
        Tuple tup = ag.getNext();
        tup.print();
      }

    } catch(NoSuchElementException e) {
      ag.close();

    } catch (TransactionAbortedException te) {
      te.printStackTrace();
      return false;

    } catch (DbException e) {
      e.printStackTrace();
      return false;
    }

    return true;
  }
}
