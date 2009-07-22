/**
 * Created on Jan 19, 2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package simpledb.net;

import simpledb.*;

import java.net.*;
import java.io.*;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Edmond Lau [edmond@mit.edu]
 *
 * NetInsert is a DB operator that inserts tuples from its child operator
 * into a network socket.
 */

public class NetInsert implements DbIterator {
	
	private String host;
	private int port;
	
	private TransactionId tid;
	private DbIterator child;
	private Socket socket;
	private DataOutputStream out;
	
	private TupleDesc td;
	private boolean insertionCompleted;
	
	public NetInsert(TransactionId tid, DbIterator child, String host, int port) {
		this.tid = tid;
		this.child = child;
		this.host = host;
		this.port = port;
		insertionCompleted = false;
		  Type type[] = new Type[1];
		  type[0] = Type.INT_TYPE;
		  td = new TupleDesc(type);
	}
	
	private void closeConnection() {
		Debug.println("NetInsert closing connection");
		try {
			if (out != null)
				out.close();
			if (socket != null)
				socket.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	/**
	 * Returns a one dimensional tuple desc to store the number of inserted tuples.
	 */
	public TupleDesc getTupleDesc() {
		  return td;
	}
	
	public void open() throws DbException, TransactionAbortedException {
		boolean connected = false;
		while (!connected) {
			try {
				Debug.println("NetInsert creating socket connection to " + host + ":" + port, 3);
				socket = Utilities.createClientSocket(new InetSocketAddress(host, port));
				Debug.println("NetInsert creating out stream", 3);
				out = new DataOutputStream(socket.getOutputStream());
				Debug.println("NetInsert opened", 3);
				connected = true;
			} catch (UnknownHostException uhe) {
				uhe.printStackTrace();
				throw new DbException(uhe);
			} catch (IOException ioe) {
				ioe.printStackTrace();
				throw new DbException(ioe);
			}
		}
		try {
			child.open();
		} catch (TransactionAbortedException tae) {
			closeConnection();
			throw tae;
		}	
	}
	
	  /**
	   * DbIterator getNext method.
	   * Inserts tuples read from child into the socket specified by the
	   * constructor.
	   *
	   * @return A 1-field tuple containing the number of inserted records.
	   * @see BufferPool#Instance
	   * @see BufferPool#insertTuple
	   * @throws NoSuchElementException When the child iterator has no more tuples.
	   */
	public Tuple getNext() throws NoSuchElementException, TransactionAbortedException {
	    // some code goes here
	  	if (insertionCompleted) {
	  		throw new NoSuchElementException("Insertion completed");
	  	}
	  	
	  	int count = 0;
	  	while (true) {
	  		try {
	    		Tuple tuple = child.getNext();
				for (int i = 0; i < tuple.getTupleDesc().numFields(); i++) {
					IntField intf = (IntField)tuple.getField(i);
					out.writeInt(intf.val());
				}
	    		out.flush();
	  			count++;
	  		} catch (IOException ioe) {
	  			ioe.printStackTrace();
				closeConnection();
	  			throw new RuntimeException("Unable to add tuple");
	  		} catch (NoSuchElementException nsee) {
	  			// no more tuples
	  			break;
	  		} catch (TransactionAbortedException tae) {
				closeConnection();
				throw tae;
			}
	  	}
	  	insertionCompleted = true;
	  	
	  	Tuple result = new Tuple(td);
	  	result.setField(0, IntField.createIntField(count));
	    return result;
	}
	
	public void close() {
		closeConnection();
		child.close();
	}
	
	public void rewind() throws DbException {
		throw new DbException("Unable to rewind NetInsert");
	}

}
