//
//  NetInsertReq.java
//  
//
//  Created by Edmond Lau on 1/23/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.requests;

import simpledb.*;
import java.io.*;

public class SpawnNetInsertReq extends SpawnOpReq {
	// host and port to insert tuples to
	public String host;
	public int port;
	
	public SpawnNetInsertReq(String host, int port, long handle) {
		this.host = host;
		this.port = port;
		this.handle = handle;
	}
	
	public String toString() {
		return "SPAWN_NET_INSERT_REQ " + handle + "," + host + ":" + port;
	}
	
	public void serialize(DataOutputStream dos) throws IOException {
		dos.writeInt(SPAWN_NET_INSERT);
		dos.writeLong(handle);
		dos.writeUTF(host);
		dos.writeInt(port);
	}
	
	public static Request read(DataInputStream dis) throws IOException {
		long handle = dis.readLong();
		String host = dis.readUTF();
		int port = dis.readInt();
		return new SpawnNetInsertReq(host, port, handle);
	}	
}
