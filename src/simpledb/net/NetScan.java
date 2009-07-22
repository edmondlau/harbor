/**
 * Created on Jan 19, 2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package simpledb.net;

import java.net.*;
import java.io.*;
import java.util.Iterator;
import java.util.NoSuchElementException;

import simpledb.*;

/**
 * @author Edmond Lau [edmond@mit.edu]
 *
 * NetScan is a DB operator that reads tuples from a network socket.
 */

public class NetScan implements DbIterator {
	
	private int port;
	private TransactionId tid;
	private ServerSocket serverSocket;
	private Socket socket;
	private DataInputStream in;
	private TupleDesc td;
	
	/**
	 * Creates a network scan over the specified port
	 */
	public NetScan(TransactionId tid, TupleDesc td, int port) {
		try {
			this.tid = tid;
			this.td = td;
			this.port = port;
			serverSocket = Utilities.createServerSocket(port);
		} catch (IOException ioe) {
			ioe.printStackTrace();
			closeConnection();
			throw new RuntimeException(ioe);
		}		
	}
	
//	public NetScan(TransactionId tid, TupleDesc td, Socket socket) {
//		this.tid = tid;
//		this.td = td;
//		this.socket = socket;
//	}
	
	private void closeConnection() {
		Debug.println("NetScan closing connection", 5);
		try {
			if (in != null)
				in.close();
			if (socket != null)
				socket.close();
			if (serverSocket != null) {
				serverSocket.close();
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	/**
	 * Opens this net scan.
	 */
	public void open() throws DbException {
		try {
			if (socket == null) {
				Debug.println("NetScan accepting @ :" + port);
				socket = Utilities.accept(serverSocket);
				Debug.println("NetScan accepted, creating input stream...");
				in = new DataInputStream(socket.getInputStream());
				Debug.println("NetScan opened");
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
			closeConnection();
			throw new DbException(ioe);
		}
	}
	
	public TupleDesc getTupleDesc() {
		return td;
	}
	
	public Tuple getNext() throws NoSuchElementException, TransactionAbortedException {
		try {
			Tuple tuple = new Tuple(td);
			for (int i = 0; i < td.numFields(); i++) {
				IntField intf = IntField.createIntField(in.readInt());
				tuple.setField(i, intf);
			}
			return tuple;
		} catch (EOFException eof) {
			throw new NoSuchElementException(eof.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			BufferPool.Instance().abortTransaction(tid);
			closeConnection();
			throw new TransactionAbortedException(e);
		}
	}
	
	public void close() {
		closeConnection();
	}
	
	public void rewind() throws DbException {
		throw new DbException("Unable to rewind NetScan");
	}
}