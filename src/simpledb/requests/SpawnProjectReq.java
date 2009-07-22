//
//  SpawnProjectReq.java
//  
//
//  Created by Edmond Lau on 2/23/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.requests;

import simpledb.*;
import java.io.*;
import java.util.*;

public class SpawnProjectReq extends SpawnOpReq {
	
	public int[] indices;
	
	public SpawnProjectReq(long handle, int[] indexes) {
		this.handle = handle;
		this.indices = new int[indexes.length];
		System.arraycopy(indexes, 0, this.indices, 0, indices.length);
	}
	
	public String toString() {
		return "SPAWN_PROJECT_REQ " + handle + ", " + Arrays.toString(indices);
	}
	
	public void serialize(DataOutputStream dos) throws IOException {
		dos.writeInt(SPAWN_PROJECT);
		dos.writeLong(handle);
		dos.writeInt(indices.length);
		for (int i = 0; i < indices.length; i++)
			dos.writeInt(indices[i]);
	}
	
	public static Request read(DataInputStream dis) throws IOException {
		long handle = dis.readLong();
		int length = dis.readInt();
		int[] indices = new int[length];
		for (int i = 0; i < length; i++) {
			indices[i] = dis.readInt();
		}
		return new SpawnProjectReq(handle, indices);
	}
}
