//
//  SpawnOpAckReq.java
//  
//
//  Created by Edmond Lau on 1/23/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.requests;

import simpledb.*;
import java.io.*;

public class OpAck implements Request {

	// handle of the newly spawned operator
	public long handle;
	
	public OpAck(long handle) {
		this.handle = handle;
	}
	
	public String toString() {
		return "OP_ACK " + handle;
	}
	
	public void serialize(DataOutputStream dos) throws IOException {
		dos.writeInt(OP_ACK);
		dos.writeLong(handle);
	}
	
	public static Request read(DataInputStream dis) throws IOException {
		long handle = dis.readLong();
		return new OpAck(handle);
	}	
}
