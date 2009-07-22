package simpledb;

import java.lang.Exception;

public class TransactionAbortedException extends Exception {
	
	public TransactionAbortedException(String s) {
	}
	
  public TransactionAbortedException() {
  }
  
  public TransactionAbortedException(Exception e) {
	  this (e.getMessage());
  }
}
