//
//  Utilities.java
//  
//
//  Created by Edmond Lau on 1/24/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb;

import java.util.*;
import java.net.*;
import java.io.*;

public class Utilities {

	public static final int RECV_BUF_SIZE = 1000000;
	public static final int SEND_BUF_SIZE = 1000000;

	private static HashMap<Integer,TupleDesc> tdPool = new HashMap<Integer,TupleDesc>();

	public static TupleDesc createTupleDesc(int numFields) {
		if (!tdPool.containsKey(numFields)) {
			Type typeAr[] = new Type[numFields];
			for (int i = 0; i < numFields; i++) {
				typeAr[i] = Type.INT_TYPE;
			}
			TupleDesc td = new TupleDesc(typeAr);
			tdPool.put(numFields, td);
		}
		return tdPool.get(numFields);
	}
	
	public static int columns(String filename) {
		String num;
		if (filename.startsWith("sf"))
			num = filename.substring(2, filename.indexOf("."));
		else
			num = filename.substring(1, filename.indexOf("."));
		return Integer.parseInt(num) + 1;	
	}
	
	public static int execute(DbIterator op) throws DbException, TransactionAbortedException {
		return execute(op, true);
	}
	
	public static int executeOpened(DbIterator op) throws DbException, TransactionAbortedException {
		return execute(op, false);
	}
	
	private static int execute(DbIterator op, boolean open) throws DbException, TransactionAbortedException {
		try {
			if (open) {
				op.open();
			}
			Tuple tuple = op.getNext();
			Debug.println("Execution resulted in " + tuple + " tuples", 2);
			IntField val = (IntField)tuple.getField(0);
			return val.val();
		} catch (NoSuchElementException e) {
			e.printStackTrace();
			throw new DbException(e.getMessage());
		} finally {
			op.close();
		}	
	
	}

	
	// assuming all tuples are numbered from 0, what would be the record id of a
	// tuple with the specified identifier? 
	public static RecordId tupleIdToRecordId(int tupleId, int tableid, TupleDesc td) {
		int tuplesPerPage = BufferPool.PAGE_SIZE / td.getSize();
		int pageno = tupleId / tuplesPerPage;
		int recordno = tupleId % tuplesPerPage;
		return new RecordId(tableid, pageno, recordno);
	}
	
	// serialization methods
	public static void serializeTuple(Tuple tuple, DataOutputStream dos) throws IOException {
		int size = tuple.getTupleDesc().numFields();
		dos.writeInt(size);
		for (int i = 0; i < size; i++) {
			IntField intf = (IntField)tuple.getField(i);
			dos.writeInt(intf.val());
		}		
	}
	
	public static Tuple readTuple(DataInputStream dis) throws IOException {
		int size = dis.readInt();
		TupleDesc td = Utilities.createTupleDesc(size);
		Tuple tuple = new Tuple(td);
		for (int i = 0; i < size; i++)
			tuple.setField(i, IntField.createIntField(dis.readInt()));
		return tuple;
	}
	
	public static void serializeRid(RecordId rid, DataOutputStream dos) throws IOException {
		dos.writeInt(rid.tableid());
		dos.writeInt(rid.pageno());
		dos.writeInt(rid.tupleno());
	}
	
	public static RecordId readRid(DataInputStream dis) throws IOException {
		int tableid = dis.readInt();
		int pageno = dis.readInt();
		int tupleno = dis.readInt();
		return new RecordId(tableid, pageno, tupleno);	
	}
	
	// socket methods
	public static Socket createClientSocket(InetSocketAddress site) throws IOException {
		Socket socket = new Socket();
		socket.setTcpNoDelay(true);	
		socket.setReceiveBufferSize(RECV_BUF_SIZE);
		socket.setSendBufferSize(SEND_BUF_SIZE);	
		socket.bind(null);
		socket.connect(site);
		return socket;
	}
	
	public static ServerSocket createServerSocket(int port) throws IOException {
		ServerSocket server = new ServerSocket(port);
		server.setReceiveBufferSize(RECV_BUF_SIZE);		
		return server;
	}
	
	public static Socket accept(ServerSocket server) throws IOException {
		Socket socket = server.accept();
		socket.setTcpNoDelay(true);	
		socket.setSendBufferSize(SEND_BUF_SIZE);
		return socket;	
	}
}
