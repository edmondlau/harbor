//
//  CommitReq.java
//  
//
//  Created by Edmond Lau on 1/23/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.requests;

import simpledb.*;

import java.io.*;

public class CommitReq implements Request {

	public int epoch;
	
	public CommitReq(int epoch) {
		this.epoch = epoch;
	}
	
	public void serialize(DataOutputStream dos) throws IOException {
		dos.writeInt(COMMIT);
		dos.writeInt(epoch);
	}
	
	public static Request read(DataInputStream dis) throws IOException {
		int epoch = dis.readInt();
		return new CommitReq(epoch);
	}
	
	public String toString() {
		return "COMMIT_REQ " + epoch;
	}
}
