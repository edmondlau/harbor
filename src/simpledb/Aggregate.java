package simpledb;

import java.util.*;

/**
 * The Aggregator operator that computes an aggregate (e.g., sum, avg, max,
 * min).  Note that we only support aggregates over a single column, grouped
 * by a single column.
 */
public class Aggregate implements DbIterator {

	private DbIterator child;
	private int aggFieldIndex;
	private int groupFieldIndex;
	private Aggregator agg;
	private TupleDesc tupleDesc;
	
	private boolean isMapInitialized;
	private Map<Field, List<Field>> groupsToVals;
	private Iterator<Map.Entry<Field, List<Field>>> mapIterator;
	
	
  /**
   * Constructor.
   * @param child The DbIterator that is spoonfeeding us tuples.
   * @param afield The column over which we are computing an aggregate.
   * @param gfield The column over which we are grouping the result.
   * @param agg The class that implements the aggregated value.
   */
  public Aggregate(DbIterator child, int afield, int gfield, Aggregator agg) {
    // some code goes here
  	this.child = child;
  	aggFieldIndex = afield;
  	groupFieldIndex = gfield;
  	this.agg = agg;
  	
  	groupsToVals = new HashMap<Field, List<Field>>();
  	
  	TupleDesc childTupleDesc = child.getTupleDesc();
  	
  	Type typeAr[] = new Type[2];
  	typeAr[0] = childTupleDesc.getType(gfield);
  	typeAr[1] = childTupleDesc.getType(afield);
  	tupleDesc = new TupleDesc(typeAr);
  }

  public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
    // some code goes here
  	child.open();
  }

  /**
   * Returns the next tuple where the first field is the field by which we are
   * grouping, and the second field is the result of computing the aggregate.
   */
  public Tuple getNext() throws NoSuchElementException, TransactionAbortedException {
    // some code goes here
  	// hash all the tuples and collect their agg fields
  	if (!isMapInitialized) {
  		Tuple tuple;
  		try {
  			while (true) {
  				tuple = child.getNext();
  				Field groupField = tuple.getField(groupFieldIndex);
  				List values;
  				if (groupsToVals.containsKey(groupField)) {
  					values = groupsToVals.get(groupField);
  					values.add(tuple.getField(aggFieldIndex));
  				} else {
  					values = new LinkedList();
  					values.add(tuple.getField(aggFieldIndex));
  					groupsToVals.put(groupField, values);
  				}
  			}
  		} catch (NoSuchElementException noee) {
  		}
  		
  		mapIterator = groupsToVals.entrySet().iterator();
  		isMapInitialized = true;
  	}
  	if (mapIterator.hasNext()) {
  		Map.Entry<Field, List<Field>> entry = mapIterator.next();
  		Tuple tuple = new Tuple(getTupleDesc());
  		tuple.setField(0, entry.getKey());
  		tuple.setField(1, agg.execute(entry.getValue()));
  		return tuple;
  	}
  	
    throw new NoSuchElementException("Aggregation complete");
  }

  public void rewind() throws DbException, TransactionAbortedException {
    // some code goes here
  	mapIterator = groupsToVals.entrySet().iterator();
  }

  /**
   * Returns the TupleDesc of this Aggregate.
   * This is always a 2-field tuple.
   */
  public TupleDesc getTupleDesc() {
    // some code goes here
    return tupleDesc;
  }

  public void close() {
    // some code goes here
  	child.close();
  	isMapInitialized = false;
  	groupsToVals = null;
  }
}
