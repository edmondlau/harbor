package simpledb;

import java.io.*;

/** A RecordID is a reference to a specific tuple on a specific page of a specific
  table 
  */
public class RecordId implements Serializable {

	private int tableid;
	private int pageno;
	private int tupleno;
	
  /** Constructor.
   * @param tableid the table that this record is in
   * @param pageno the page of tableid that this record is in
   * @param tupleno the tuple number within the page.
   */
  public RecordId(int tableid, int pageno, int tupleno) {
    // some code goes here
  	this.tableid = tableid;
  	this.pageno = pageno;
  	this.tupleno = tupleno;
  }

  /**
   * @return the page this RecordId references.
   */
  public int pageno() {
    // some code goes here
    return pageno;
  }

  /**
   * @return the tuple number this RecordId references.
   */
  public int tupleno() {
    // some code goes here
    return tupleno;
  }

  /**
   * @return the table id this RecordId references.
   */
  public int tableid() {
    // some code goes here
    return tableid;
  }
  
  public PageId pageid() {
	return new PageId(tableid, pageno);
  }
  
  public String toString() {
	return "[" + tableid + "," + pageno + "," + tupleno + "]";
  }
  
//  public boolean equals(Object o) {
//	if (!(o instanceof RecordId))
//		return false;
//	RecordId rid = (RecordId)o;
//	return rid.tableid == tableid && rid.pageno == pageno && rid.tupleno == tupleno;
//  }
//  
//  public int hashCode() {
//	
//  }
}
