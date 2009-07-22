package simpledb;
import java.util.*;
import java.io.*;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
public class Insert extends AbstractUpdate {
	private int tableId;

	/**
   * Constructor.
   * @param t The transaction running the insert.
   * @param child The child operator to read tuples to be inserted from.
   * @param tableid The table to insert tuples into.
   */
  public Insert(TransactionId t, DbIterator child, int tableid) {
	super(t, child);
  	tableId = tableid;
  }

  public void process(Tuple tuple) throws IOException, DbException, TransactionAbortedException {
	BufferPool.Instance().insertTuple(tid, tableId, tuple);
  }
}
