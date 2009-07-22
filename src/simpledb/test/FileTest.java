/**
 * Created on Sep 29, 2005
 * Author: Edmond Lau
 */
package simpledb.test;

import simpledb.*;
import java.io.*;
import java.util.*;

public class FileTest extends Test {

	public boolean runTest(String args[]) {
  	
	  // construct a 3-column table schema
	  Type typeAr[] = new Type[3];
	  typeAr[0] = Type.INT_TYPE;
	  typeAr[1] = Type.INT_TYPE;
	  typeAr[2] = Type.INT_TYPE;
	  TupleDesc td = new TupleDesc(typeAr);
	  
	  // create the table, associate it with some_data_file.dat
	  // and tell the catalog about the schema of this table.
	  HeapFile table1 = new HeapFile(new File("some_data_file.dat"));
	  Catalog.Instance().addTable(table1, td);
	  
	  PageId pid = new PageId(table1.id(), 0);
	  HeapPage page = (HeapPage)table1.readPage(pid);
	  TransactionId tid = new TransactionId();                                
	  SeqScan ss = new SeqScan(tid, table1.id());
	  
	  try {
	  	ss.open();
	  	try {
	  		while(true) {
	  			Tuple tuple = ss.getNext();
	  			tuple.print();
	  		}
	  	} catch (NoSuchElementException noee) {
	  		ss.close();
	  	}
	  } catch (TransactionAbortedException tae) {
	  	tae.printStackTrace();
	  } catch (DbException de) {
	  	de.printStackTrace();
	  }
		
  	return true;
  }
}
