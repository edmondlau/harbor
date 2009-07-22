package simpledb;

import java.util.*;
import java.io.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
	
	private Type[] types;
	private int size;
	
  /**
   * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
   * with the first td1.numFields coming from td1 and the remaining from td2
   * @param td1 The TupleDesc with the first fields of the new TupleDesc
   * @param td2 The TupleDesc with the last fields of the TupleDesc
   * @return the new TupleDesc
   */
  public static TupleDesc combine(TupleDesc td1, TupleDesc td2) {
    Type[] typeAr = new Type[td1.numFields() + td2.numFields()];
    for (int i = 0; i < td1.numFields(); i++) {
    	typeAr[i] = td1.getType(i);
    }
    for (int j = 0; j < td2.numFields(); j++) {
    	typeAr[td1.numFields() + j] = td2.getType(j);
    }
    return new TupleDesc(typeAr);
  }

  /**
   * Constructor.
   * Create a new tuple desc with typeAr.length fields 
   * with fields of the specified types.
   * @param typeAr array specifying the number of and types of fields in this TupleDesc
   */
  public TupleDesc(Type[] typeAr) {
    // some code goes here
  	types = new Type[typeAr.length];
  	for (int i = 0; i < typeAr.length; i++) {
  		types[i] = typeAr[i];
  		size += typeAr[i].getLen();
  	}
  }

  /**
   * @return the number of fields in this TupleDesc
   */
  public int numFields() {
    // some code goes here
    return types.length;
  }

  /**
   * Gets the type of the ith field of this TupleDesc.
   * @param i The index of the field to get the type of
   * @return the type of the ith field
   * @throws NoSuchElementException if i is not a valid field reference.
   */
  public Type getType(int i) throws NoSuchElementException{
    // some code goes here
  	if (i >= types.length)
  		throw new NoSuchElementException("i = " + i);
    return types[i];
  }

  /**
   * @return The size (in bytes) of tuples corresponding to this TupleDesc.
   * Note that tuples from a given TupleDesc are of a fixed size.
   */
  public int getSize() {
    // some code goes here
    return size;
  }

	public String toString() {
		StringBuffer buf = new StringBuffer();
		for (int i = 0; i < types.length; i++) {
			if (types[i].equals(Type.INT_TYPE)) {
				buf.append("int, ");
			} else {
				buf.append("unknown, ");
			}
		}
		return buf.toString();
	}

  /**
   * Returns whether or not two TupleDesc are equal.
   * Two TupleDescs are considered equal if they are the same size and if the
   * n-th type in this TupleDesc is equal to the n-th type in td.
   */
  public boolean equals(TupleDesc td) {
    // some code goes here
  	if (numFields() != td.numFields())
  		return false;
  	
		for (int i = 0; i < numFields(); i++) {
			if (!getType(i).equals(td.getType(i)))
				return false;
		}
		return true;
  }
  
  public int hashCode() {	
  	int code = 0;
  	for (int i = 0; i < types.length; i++) {
  		code += types[i].hashCode();
  	}
  	return code;
  }
}
