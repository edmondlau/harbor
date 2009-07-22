//
//  SpawnDeleteReq.java
//  
//
//  Created by Edmond Lau on 1/23/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.requests;

import simpledb.*;
import java.io.*;

public class SpawnDeleteReq extends SpawnOpReq implements ISpawnUpdateReq {
	
	public SpawnDeleteReq(long handle) {
		this.handle = handle;
	}
	
	public String toString() {
		return "SPAWN_DELETE_REQ " + handle;
	}
	
	public void serialize(DataOutputStream dos) throws IOException {
		dos.writeInt(SPAWN_DELETE);
		dos.writeLong(handle);
	}
	
	public static Request read(DataInputStream dis) throws IOException {
		long handle = dis.readLong();
		return new SpawnDeleteReq(handle);
	}
}
