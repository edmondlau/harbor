/**
 * Created on Jan 19, 2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package simpledb.test;

import simpledb.*;
import simpledb.net.*;

public class NetTest extends Test {
	
  public boolean runTest(String args[]) {
	  try {
		  int port = 8888;
		  
		  int cols = Integer.parseInt(args[1]) - 1;
		  int tableid = file[cols][0].id();
		  TransactionId tid = new TransactionId();
		  NetWriter writer = new NetWriter(tid, tableid, "localhost", port);
		  NetReader reader = new NetReader(tid, tableid, port);

		  reader.start();
		  writer.start();
	  } catch (DbException db) {
		  db.printStackTrace();
		  return false;
	  }
	  
	  return true;
  }
  
  class NetReader extends Thread {
	 
	  NetScan ns;
	  TransactionId tid;
	  
	  public NetReader(TransactionId tid, int tableid, int port) throws DbException {
		  ns = new NetScan(tid, Catalog.Instance().getTupleDesc(tableid), port);
	  }
	  
	  public void run() {
		  try {
			  dump(tid, ns);
		  } catch (TransactionAbortedException tae) {
			  tae.printStackTrace();
		  }
		  System.out.println("NetReader done");
	  }
  }
  
  class NetWriter extends Thread {

	  NetInsert ni;
	  TransactionId tid;
	  int tableid;
	  String host;
	  int port;

	  public NetWriter(TransactionId tid, int tableid, String host, int port) throws DbException {
		  this.tid = tid;
		  this.tableid = tableid;
		  this.host = host;
		  this.port = port;
	  }
	  
	  public void run() {
		  try {
			  SeqScan ss = new SeqScan(tid, tableid);
			  NetInsert ni = new NetInsert(tid, ss, host, port);
			  ni.open();
			  ni.getNext();
			  ni.close();
			  //dump(tid, ni);
		  } catch (TransactionAbortedException tae) {
			  tae.printStackTrace();
		  }  catch (DbException db) {
			  db.printStackTrace();
		  }
		  System.out.println("NetWriter done");
	  }
  }
}
