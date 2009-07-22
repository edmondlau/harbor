package simpledb;

import java.io.*;

/**
 * Tuple maintains information about the contents of a tuple.
 * Tuples have a specified schema specified by a TupleDesc object and contain
 * Field objects with the data for each field.
 */
public class Tuple implements Serializable {
	private TupleDesc td;
	private Field[] fields;
	private RecordId rid;
  /**
   * Constructor.
   * Create a new tuple with the specified schema (type).
   * @param td The schema of this tuple.
   */
  public Tuple(TupleDesc td) {
    // some code goes here
  	this.td = td;
  	fields = new Field[td.numFields()];
  	for (int i = 0; i < fields.length; i++) {
  		if (td.getType(i).equals(Type.INT_TYPE))
  			fields[i] = IntField.createIntField(0);
  		else
  			throw new RuntimeException("Unrecognized type");
  	}
  }
  
  /**
   * Constructor.
   * Create a copy of the tuple from the specified tuple.
   */
  public Tuple(Tuple t) {
  	// TupleDesc and Fields are immutable
  	td = t.td;
  	fields = new Field[td.numFields()];
  	for (int i = 0; i < fields.length; i++) {
  		fields[i] = t.fields[i];
  	}
  }

  /**
   * Merge two Tuples into one.
   */
  public static Tuple combine(Tuple tuple1, Tuple tuple2) {
  	TupleDesc td1 = tuple1.getTupleDesc();
  	TupleDesc td2 = tuple2.getTupleDesc();
  	TupleDesc tupleDesc = TupleDesc.combine(td1, td2);

  	Tuple combinedTuple = new Tuple(tupleDesc);
    for (int i = 0; i < td1.numFields(); i++) {
    	combinedTuple.setField(i, tuple1.getField(i));
    }
    for (int j = 0; j < td2.numFields(); j++) {
    	combinedTuple.setField(j + td1.numFields(), tuple2.getField(j));
    }
    return combinedTuple;
  }
  
  /**
   * @return The TupleDesc representing the schema of this tuple.
   */
  public TupleDesc getTupleDesc() {
    // some code goes here
    return td;
  }

  /**
   * @return The RecordID representing the location of this this tuple on
   * disk.  May be null.
   */
  public RecordId getRecordId() {
    // some code goes here
    return rid;
  }

  /**
   * @return Set the RecordID information for this tuple.
   * @param rid The new RecordID of this tuple.
   */
  public void setRecordId(RecordId rid) {
    // some code goes here
  	this.rid = rid;
  }

  /**
   * Change the value of the ith field of this tuple.
   * @param i The index of the field to change
   * @param f The value to change it to.
   */
  public void setField(int i, Field f) {
    // some code goes here
  	fields[i] = f;
  }

  /**
   * Return the value of the ith field.
   */
  public Field getField(int i) {
    // some code goes here
    return fields[i];
  }

  /**
   * Dumps the contents of this Tuple on standard out.
   * Note that to pass the grader.pl script, the format needs to be as
   * follows:
   * column1\tcolumn2\tcolumn3\t...\tcolumnN
   * column1\tcolumn2\tcolumn3\t...\tcolumnN
   * column1\tcolumn2\tcolumn3\t...\tcolumnN
   *
   * where \t is any whitespace, except newline.
   */
  public void print() {
  	System.out.println(this);
  }
  
  public String toString() {
	StringBuffer buf = new StringBuffer();
  	for (int i = 0; i < fields.length; i++) {
  		buf.append(fields[i].toString());
  		if (i != fields.length - 1)
  			buf.append(" ");
  	}
	return buf.toString();
  }
}
