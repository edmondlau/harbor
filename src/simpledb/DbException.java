package simpledb;

import java.lang.Exception;

/** Generic database exception class */
public class DbException extends Exception {
  public DbException(String s) {
  }
  
  public DbException(Exception e) {
	  this(e.getMessage());
  }
}
