//
//  SpawnVersionedUpdateReq.java
//  
//
//  Created by Edmond Lau on 2/16/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.requests;

import simpledb.*;
import java.io.*;

// a request to update the tuple located at the specified record id
// to tuple.
public class SpawnVersionedUpdateReq extends SpawnOpReq implements ISpawnUpdateReq {

	public int tupleId;
	public Tuple tuple;

	public SpawnVersionedUpdateReq(long handle, int tupleId, Tuple tuple) {
		super.handle = handle;
		this.tupleId = tupleId;
		this.tuple = tuple;
	}
	
	public String toString() {
		return "SPAWN_VERSIONED_UPDATE_REQ " + handle + ", " + tupleId + "," + tuple;
	}

	public void serialize(DataOutputStream dos) throws IOException {
		dos.writeInt(SPAWN_VERSIONED_UPDATE);
		dos.writeLong(handle);
		dos.writeInt(tupleId);
		Utilities.serializeTuple(tuple, dos);
	}
	
	public static Request read(DataInputStream dis) throws IOException {
		long handle = dis.readLong();
		int tupleId = dis.readInt();
		Tuple tuple = Utilities.readTuple(dis);
		return new SpawnVersionedUpdateReq(handle, tupleId, tuple);
	}
}
