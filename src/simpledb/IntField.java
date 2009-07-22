package simpledb;

import java.io.*;
import java.util.*;

/**
 * Instance of Field that stores a single integer .
 */
public class IntField implements Field {
  int myVal;

	private static int POOL_SIZE = 100000;
	private static HashMap<Integer, IntField> pool = new HashMap<Integer, IntField>(POOL_SIZE);
	private static LinkedList<Integer> poolKeys = new LinkedList<Integer>();
	private static Random rand = new Random();

  /**
   * Constructor
   * @param i The value of this field.
   */
  private IntField(Integer i) {
    this.myVal = i.intValue();
  }

	public static IntField createIntField(Integer i) {
		if (!pool.containsKey(i)) {
			if (pool.size() >= POOL_SIZE) {
				Integer evict = poolKeys.remove();
				pool.remove(evict);
			}
			pool.put(i, new IntField(i));
			poolKeys.add(i);
		}
		return pool.get(i);
	}

  public String toString() {
    return new Integer(myVal).toString();
  }

  public int hashCode() {
    return myVal;
  }
  
  public int val() {
  	return myVal;
  }
  
  public boolean equals(Object field) {
    return ((IntField) field).myVal == myVal;
  }

  public void serialize(DataOutputStream dos) throws IOException {
    dos.writeInt(myVal);
  }

  /**
   * Compare the specified field to the value of this Field.
   * Return semantics are as specified by Field.compare
   * @throws IllegalCastException if val is not an IntField 
   * @see Field#compare
   */
  public boolean compare(int op, Field val) {

    IntField iVal = (IntField) val;

    if(op == Predicate.EQUALS)
      return myVal == iVal.myVal;

    else if(op == Predicate.GREATER_THAN)
      return myVal > iVal.myVal;

    else if(op == Predicate.GREATER_THAN_OR_EQ)
      return myVal >= iVal.myVal;

    else if(op == Predicate.LESS_THAN)
      return myVal < iVal.myVal;

    else if(op == Predicate.LESS_THAN_OR_EQ)
      return myVal <= iVal.myVal;

    return false;
  }
}
