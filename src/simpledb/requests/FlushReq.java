//
//  FlushReq.java
//  
//
//  Created by Edmond Lau on 3/3/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.requests;

import simpledb.*;
import java.io.*;

public class FlushReq implements Request {

	public void serialize(DataOutputStream dos) throws IOException {
		dos.writeInt(FLUSH);
	}
	
	public static Request read(DataInputStream dis) {
		return new FlushReq();
	}

	public String toString() {
		return "FLUSH_REQ";
	}
}