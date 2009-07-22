//
//  AcquireLockReq.java
//  
//
//  Created by Edmond Lau on 1/31/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.requests;

import simpledb.*;
import java.io.*;

public class AcquireTableLockReq implements Request {

	public TransactionId tid;
	public String filename;

	public AcquireTableLockReq(TransactionId tid, String filename) {
		this.tid = tid;
		this.filename = filename;
	}
	
	public void serialize(DataOutputStream dos) throws IOException {
		dos.writeInt(ACQUIRE_TABLE_LOCK);
		dos.writeInt(tid.value());
		dos.writeUTF(filename);
	}
	
	public static Request read(DataInputStream dis) throws IOException {
		TransactionId tid = new TransactionId(dis.readInt());
		String filename = dis.readUTF();
		return new AcquireTableLockReq(tid, filename);
	}
	
	public String toString() {
		return "ACQUIRE_TABLE_LOCK_REQ: " + tid + ", " + filename;
	}
}
