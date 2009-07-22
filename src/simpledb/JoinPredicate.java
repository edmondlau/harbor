package simpledb;

/**
 * JoinPredicate compares fields of two tuples using a predicate.
 * JoinPredicate is most likely used by the Join operator.
 */
public class JoinPredicate {

	private int fieldIndex1;
	private int fieldIndex2;
	private int op;
	
  /**
   * Constructor -- create a new predicate over two fields of two tuples.
   * @param field1 The field index into the first tuple in the predicate
   * @param field2 The field index into the second tuple in the predicate
   * @param op The operation to apply (as defined in Predicate); either
   * Predicate.GREATER_THAN, Predicate.LESS_THAN, Predicate.EQUAL,
   * Predicate.GREATER_THAN_OR_EQ, or Predicate.LESS_THAN_OR_EQ
   * @see Predicate
   */
  public JoinPredicate(int field1, int op, int field2) {
    // some code goes here
  	fieldIndex1 = field1;
  	fieldIndex2 = field2;
  	this.op = op;
  	if (op < Predicate.EQUALS || op > Predicate.GREATER_THAN_OR_EQ)
  		throw new RuntimeException("Invalid op passed to JoinPredicate");
  }

  /**
   * Apply the predicate to the two specified tuples.
   * @return true if the tuples satisfy the predicate.
   */
  public boolean filter(Tuple t1, Tuple t2) {
    // some code goes here
  	Field field1 = t1.getField(fieldIndex1);
  	Field field2 = t2.getField(fieldIndex2);
    return field1.compare(op, field2);
  }
}
