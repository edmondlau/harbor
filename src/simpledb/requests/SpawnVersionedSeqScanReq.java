//
//  SpawnVersionedSeqScanReq.java
//  
//
//  Created by Edmond Lau on 2/16/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//

package simpledb.requests;

import simpledb.*;
import java.io.*;

public class SpawnVersionedSeqScanReq extends SpawnOpReq {
	
	public int hwm;
	
	public SpawnVersionedSeqScanReq(long handle, int hwm) {
		this.handle = handle;
		this.hwm = hwm;
	}
	
	public String toString() {
		return "SPAWN_VERSIONED_SEQ_SCAN_REQ " + handle + " hwm " + hwm;
	}
	
	public void serialize(DataOutputStream dos) throws IOException {
		dos.writeInt(SPAWN_DELETE);
		dos.writeLong(handle);
		dos.writeInt(hwm);
	}
	
	public static Request read(DataInputStream dis) throws IOException {
		long handle = dis.readLong();
		int hwm = dis.readInt();
		return new SpawnVersionedSeqScanReq(handle, hwm);
	}
}
