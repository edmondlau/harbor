//
//  GrantLockReq.java
//  
//
//  Created by Edmond Lau on 2/2/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.requests;

import java.io.*;

public class GrantLockAck implements Request {

	public void serialize(DataOutputStream dos) throws IOException {
		dos.writeInt(GRANT_LOCK_ACK);
	}
	
	public static Request read(DataInputStream dis) throws IOException {
		return new GrantLockAck();
	}

}
