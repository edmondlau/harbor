//
//  BeginReq.java
//  
//
//  Created by Edmond Lau on 1/23/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.requests;

import simpledb.*;
import simpledb.net.CoordinatorHub.*;

import java.io.*;

public class BeginReq implements Request {

	public final static boolean SNAPSHOT = true;

	public TransactionId tid;

	public BeginReq(TransactionId tid) {
		this.tid = tid;
	}
	
	public void serialize(DataOutputStream dos) throws IOException {
		dos.writeInt(BEGIN);
		dos.writeInt(tid.value());
	}
	
	public static Request read(DataInputStream dis) throws IOException {
		TransactionId tid = new TransactionId(dis.readInt());
		return new BeginReq(tid);
	}
	
	public String toString() {
		return "BEGIN_REQ " + tid;
	}
}
