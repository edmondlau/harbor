package simpledb;
import java.util.*;

/** Filter is an operator that implements a relational select */
public class Filter implements DbIterator {

	private Predicate predicate;
	private DbIterator child;
	
  /** Constructor accepts a predicate to apply and a child
    operator to read tuples to filter from.
    @param p The predicate to filter tuples with
    @param child The child operator
    */
  public Filter(Predicate p, DbIterator child) {
    // some code goes here
  	predicate = p;
  	this.child = child;
  }

  public TupleDesc getTupleDesc() {
    // some code goes here
    return child.getTupleDesc();
  }

  public void open() throws DbException, NoSuchElementException, TransactionAbortedException {
    // some code goes here
  	child.open();
  }

  public void close() {
    // some code goes here
  	child.close();
  }

  public void rewind() throws DbException, TransactionAbortedException {
    // some code goes here
  	child.rewind();
  }

  /** DbIterator.getNext implementation.
    Iterates over tuples from the child operator, applying the
    predicate to them and returning those that pass the predicate
    (i.e. for which the Predicate.filter() returns true.)

    @return The next tuple that passes the filter
    @throws NoSuchElementException if there are no more tuples to filter
    @see Predicate#filter
    */
  public Tuple getNext() throws NoSuchElementException, TransactionAbortedException {
    // some code goes here
  	Tuple tuple;
  	
  	// loop terminates when child.getNext() throws NoSuchElementException
  	while (true) {
  		tuple = child.getNext();
  		if (predicate.filter(tuple)) {
  			return tuple;
  		}
  	}
  }
}
