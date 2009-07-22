//
//  NetScanReq.java
//  
//
//  Created by Edmond Lau on 1/23/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.requests;

import simpledb.*;
import java.io.*;

public class SpawnNetScanReq extends SpawnOpReq {
	// port to read tuples from
	public int port;
	
	// tuple descriptor of incoming tuples
	public TupleDesc td;
	
	public SpawnNetScanReq(TupleDesc td, int port) {
		this.td = td;
		this.port = port;
	}
	
	public String toString() {
		return "SPAWN_NET_SCAN_REQ " + td + ", " + port;
	}
	
	public void serialize(DataOutputStream dos) throws IOException {
		dos.writeInt(SPAWN_NET_SCAN);
		dos.writeInt(port);
		dos.writeInt(td.numFields());
	}
	
	public static Request read(DataInputStream dis) throws IOException {
		int port = dis.readInt();
		TupleDesc td = Utilities.createTupleDesc(dis.readInt());
		return new SpawnNetScanReq(td, port);
	}
}
