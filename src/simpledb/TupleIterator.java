package simpledb;

import simpledb.*;
import java.util.*;

/**
 * Implements a DbIterator over a Set of tuples.
 * Every time someone calls getNext() on this DbIterator, it returns the next
 * tuple from a set that was passed to it at constructor time.
 */
public class TupleIterator implements DbIterator {
  Iterator i = null;
  TupleDesc td = null;
  List tuples = null;

  public TupleIterator(Tuple tuple) throws DbException {
	tuples = new LinkedList();
	tuples.add(tuple);
	td = tuple.getTupleDesc();
  }

  /**
   * Constructor.
   * @param tuples The set of tuples to iterate over
   */
  public TupleIterator(TupleDesc td, List tuples) throws DbException {
    this.td = td;
    this.tuples = new LinkedList(tuples);

    // check that all tuples are the right TupleDesc
    Iterator x = tuples.iterator();
    while(x.hasNext()) {
      Tuple t = (Tuple) x.next();
      if(!t.getTupleDesc().equals(td))
        throw new DbException("incompatible tuple in tuple set");
    }
  }

  public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
    i = tuples.iterator();
  }

  public Tuple getNext() throws TransactionAbortedException {
    if(i == null || !i.hasNext())
      throw new NoSuchElementException();
    return (Tuple) i.next();
  }

  public void rewind() throws DbException, TransactionAbortedException {
    close();
    open();
  }

  public TupleDesc getTupleDesc() {
    return td;
  }

  public void close() {
    i = null;
  }
}
