package simpledb;

/** PageId is a reference to a specific page of a specific table. */
public class PageId {

	private int tableId;
	private int pageNumber;
	
  /** Constructor. Create a page id structure for a specific page of a specific table
    @param tableId The table that is being referenced
    @param pgNo The page number in that table.
    */
  public PageId(int tableId, int pgNo) {
    // some code goes here
  	this.tableId = tableId;
  	pageNumber = pgNo;
  }

  /** @return the table associated with this PageId */
  public int tableid() {
    // some code goes here
    return tableId;
  }

  /** @return the page number in the table tableid() associated with this PageId */
  public int pageno() {
    // some code goes here
    return pageNumber;
  }

  /** @return a hash code for this page, represented by the concatenation of the 
    table number and the page number (needed if a PageId is used as a key in 
    a hash table in the BufferPool, for example.)
    @see BufferPool 
    */
  public int hashCode() {
    // some code goes here
    return 7*pageNumber + 13*tableId;
  }

  /** Compares one PageId to another.
    @param o The object to compare against (must be a PageId)
    @return true if the objects are equal (e.g., page numbers and table ids are the same)
    */
  public boolean equals(Object o) {
    // some code goes here
  	if (!(o instanceof PageId))
  		return false;
  	PageId other = (PageId)o;
    return (pageNumber == other.pageNumber && tableId == other.tableId);
  }
  
  public String toString() {
  	return "{" + tableId + "," + pageNumber + "}";
  }
}

