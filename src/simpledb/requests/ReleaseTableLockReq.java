//
//  ReleaseTableLockReq.java
//  
//
//  Created by Edmond Lau on 2/2/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.requests;

import simpledb.*;
import java.io.*;

public class ReleaseTableLockReq implements Request {

	public TransactionId tid;

	public ReleaseTableLockReq(TransactionId tid) {
		this.tid = tid;
	}
	
	public String toString() {
		return "RELEASE_TABLE_LOCK_REQ: " + tid;
	}
	
	public void serialize(DataOutputStream dos) throws IOException {
		dos.writeInt(RELEASE_TABLE_LOCK);
		dos.writeInt(tid.value());
	}
	
	public static Request read(DataInputStream dis) throws IOException {
		TransactionId tid = new TransactionId(dis.readInt());
		return new ReleaseTableLockReq(tid);
	}
}
