//
//  SpawnInsertReq.java
//  
//
//  Created by Edmond Lau on 1/23/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.requests;

import simpledb.*;
import java.io.*;

public class SpawnVersionedInsertReq extends SpawnOpReq implements ISpawnUpdateReq {
	
	// the tuple to insert
	public Tuple tuple;
	
	public SpawnVersionedInsertReq(long handle, Tuple tuple) {
		this.handle = handle;
		this.tuple = tuple;
	}
	
	public String toString() {
		return "SPAWN_VERSIONED_INSERT_REQ " + handle + ", " + tuple;
	}
	
	public void serialize(DataOutputStream dos) throws IOException {
		dos.writeInt(SPAWN_VERSIONED_INSERT);
		dos.writeLong(handle);
		Utilities.serializeTuple(tuple, dos);
	}
	
	public static Request read(DataInputStream dis) throws IOException {
		long handle = dis.readLong();
		Tuple tuple = Utilities.readTuple(dis);
		return new SpawnVersionedInsertReq(handle, tuple);
	}	
}
