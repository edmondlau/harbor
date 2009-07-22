//
//  ExecuteReq.java
//  
//
//  Created by Edmond Lau on 1/23/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.requests;

import simpledb.*;
import java.io.*;

public class ExecuteReq implements Request {
	public long handle;
	
	public ExecuteReq(long handle) {
		this.handle = handle;
	}
	
	public void serialize(DataOutputStream dos) throws IOException {
		dos.writeInt(EXECUTE);
		dos.writeLong(handle);
	}
	
	public static Request read(DataInputStream dis) throws IOException {
		long handle = dis.readLong();
		return new ExecuteReq(handle);
	}
	
	public String toString() {
		return "EXECUTE_REQ " + handle;
	}
}
