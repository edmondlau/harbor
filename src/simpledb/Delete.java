package simpledb;
import java.util.*;
import java.io.*;

/** The delete operator.  Delete reads tuples
  from its child operator and removes them
  from the table they belong to.

*/

public class Delete extends AbstractUpdate {

  /**
   * Constructor specifying the transaction that this delete belongs to as
   * well as the child to read from.
   * @param t The transaction this delete runs in
   * @param child The child operator to read tuples for deletion from
   */
  public Delete(TransactionId t, DbIterator child) {
	super(t, child);
  }
  
  public void process(Tuple tuple) throws IOException, DbException, TransactionAbortedException  {
	BufferPool.Instance().deleteTuple(tid, tuple);
  }
}
