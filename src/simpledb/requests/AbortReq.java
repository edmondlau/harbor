//
//  AbortReq.java
//  
//
//  Created by Edmond Lau on 1/23/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.requests;

import simpledb.*;
import java.io.*;

public class AbortReq implements Request {

	public void serialize(DataOutputStream dos) throws IOException {
		dos.writeInt(ABORT);
	}
	
	public static Request read(DataInputStream dis) {
		return new AbortReq();
	}

	public String toString() {
		return "ABORT_REQ";
	}
}
