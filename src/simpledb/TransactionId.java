package simpledb;

import java.io.*;

/** TransactionId is a class that contains the identifier of
    a transaction.  Currently unimplemented.
*/
public class TransactionId implements Serializable {
  static int counter = 0;

  int myid;
  
//  private Thread transactionThread;
  
  public TransactionId() {
    myid = counter++;
//    transactionThread = Thread.currentThread();
  }
  
  public TransactionId(int tid) {
	myid = tid;
  }
  
  public int value() {
	return myid;
  }
  
  public String toString() {
	  return "" + myid;
  }
  
  public boolean equals(Object o) {
	return (o instanceof TransactionId) && (((TransactionId)o).myid == myid);
  }

  public int hashCode() {
	return myid;
  }
  
//  public Thread getThread() {
//  	return transactionThread;
//  }
}
