package simpledb;

import java.io.*;

/** Predicate compares tuples to a specified Field value.
* This should really be an interface.
*/
public class Predicate implements Serializable {

  /** Constant for EQUALS operation and return code from Field.compare */
  public static final int EQUALS = 0;  

  /** Constant for GREATER_THAN operation and return code from Field.compare */
  public static final int GREATER_THAN = 1;

  /** Constant for LESS_THAN operation and return code from Field.compare */
  public static final int LESS_THAN = 2;

  /** Constant for LESS_THAN_OR_EQ operation */
  public static final int LESS_THAN_OR_EQ = 3;

  /** Constant for GREATER_THAN_OR_EQ operation */
  public static final int GREATER_THAN_OR_EQ = 4;

  private int fieldIndex;
  private int op;
  private Field operand;
  
  public int getFieldIndex() { return fieldIndex; }
  public int getOp() { return op; }
  public Field getOperand() { return operand; }
  
  // hack for ConjunctivePredicate
  protected Predicate() {}
  
  /** Constructor.
   * @param field field number of passed in tuples to compare against.
   * @param op operation to use for comparison
   * @param operand field value to compare passed in tuples to
   */
  public Predicate(int field, int op, Field operand) {
    // some code goes here
  	fieldIndex = field;
  	this.op = op;
  	this.operand = operand;
  	if (op < Predicate.EQUALS || op > Predicate.GREATER_THAN_OR_EQ)
  		throw new RuntimeException("Invalid op passed to Predicate");
  }

  /**
   * Compares the field number of t specified in the constructor to the
   * operand field specified in the constructor using the operator specific in
   * the constructor.
   * @param t The tuple to compare against
   * @return true if the comparison is true, false otherwise.
   */
  public boolean filter(Tuple t) {
    // some code goes here
  	Field otherOperand = t.getField(fieldIndex);
    //return operand.compare(op, otherOperand);
  	return otherOperand.compare(op, operand);
  }
  
  public static String opToString(int op) {
	switch(op) {
		case EQUALS: return "=";
		case GREATER_THAN: return ">";
		case LESS_THAN: return "<";
		case LESS_THAN_OR_EQ: return "<=";
		case GREATER_THAN_OR_EQ: return ">=";
		default: return "?";
	}
  }
  
  public String toString() {
	return "[col" + fieldIndex + "] " + opToString(op) + " " + operand;
  }
}
