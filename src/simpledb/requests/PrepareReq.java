//
//  PrepareReq.java
//  
//
//  Created by Edmond Lau on 1/23/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.requests;

import simpledb.*;
import java.io.*;

public class PrepareReq implements Request{
	public String toString() {
		return "PREPARE";
	}
	
	public void serialize(DataOutputStream dos) throws IOException {
		dos.writeInt(PREPARE);
	}
	
	public static Request read(DataInputStream dis) throws IOException {
		return new PrepareReq();
	}
}
