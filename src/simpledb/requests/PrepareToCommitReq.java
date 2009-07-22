//
//  PrepareToCommitReq.java
//  
//
//  Created by Edmond Lau on 3/15/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.requests;

import simpledb.*;

import java.io.*;

public class PrepareToCommitReq implements Request {

	public int epoch;
	
	public PrepareToCommitReq(int epoch) {
		this.epoch = epoch;
	}
	
	public void serialize(DataOutputStream dos) throws IOException {
		dos.writeInt(PREPARE_TO_COMMIT);
		dos.writeInt(epoch);
	}
	
	public static Request read(DataInputStream dis) throws IOException {
		int epoch = dis.readInt();
		return new PrepareToCommitReq(epoch);
	}
	
	public String toString() {
		return "PREPARE_TO_COMMIT_REQ " + epoch;
	}
}
