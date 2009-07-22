package simpledb;

import java.text.ParseException;
import java.io.*;

/** Class representing a type in SimpleDB.
 * Types are static objects defined by this class;  hence, 
 * the Type constructor is private.
 */
public class Type implements Serializable {
  int typeId;
  int len;

  private Type(int typeId) {
    this.typeId = typeId;

  }

  /**
   * @return the number of bytes required to store a field of this type.
   */
  public int getLen() {
    switch (typeId) {
      case INT_ID:
        return 4;
      default:
        return 0;
    }
  }

  /**
   * @return a Field object of the same type as this object that has contents
   * read from the specified DataInputStream.
   * @param dis The input stream to read from
   * @throws ParseException if the data read from the input stream is not of the appropriate type.
   */
  public Field parse(DataInputStream dis) throws ParseException {
    try {
      switch (typeId) {
        case INT_ID:
          return IntField.createIntField(new Integer(dis.readInt()));
        default:
          return null;
      }
    } catch (IOException e) {
      throw new ParseException("couldn't parse", 0);
    }
  }

  /**
   * @return true if the specified type is the same as the tupe of this object
   */
  public boolean equals(Type t) {
    return t.typeId == typeId;
  }

  static final int INT_ID = 1;

  /**
   * Type object representing an integer (the only type currently supported by
   * SimpleDB
   */
  public static final Type INT_TYPE = new Type(INT_ID);
}
