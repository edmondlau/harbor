//
//  SpawnIndexedSeqScanReq.java
//  
//
//  Created by Edmond Lau on 3/2/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.requests;

import simpledb.*;
import java.io.*;

public class SpawnIndexedSeqScanReq extends SpawnOpReq {
	
	public int insertLow;
	public int insertHigh;
	public int deleteLow;
	public int hwm;
	public boolean seeDeleted;
	
	public SpawnIndexedSeqScanReq(long handle, int insertLow, int insertHigh, int deleteLow, int hwm, boolean seeDeleted) {
		this.handle = handle;
		this.insertLow = insertLow;
		this.insertHigh = insertHigh;
		this.deleteLow = deleteLow;
		this.hwm = hwm;
		this.seeDeleted = seeDeleted;
	}
	
	public String toString() {
		return "SPAWN_INDEXED_SEQ_SCAN_REQ " + handle + ", " + insertLow + ", " + insertHigh + 
			", " + deleteLow + ", " + hwm + ", " + seeDeleted;
	}
	
	public void serialize(DataOutputStream dos) throws IOException {
		dos.writeInt(SPAWN_INDEXED_SEQ_SCAN);
		dos.writeLong(handle);
		dos.writeInt(insertLow);
		dos.writeInt(insertHigh);
		dos.writeInt(deleteLow);
		dos.writeInt(hwm);
		dos.writeBoolean(seeDeleted);
	}
	
	public static Request read(DataInputStream dis) throws IOException {
		long handle = dis.readLong();
		int insertLow = dis.readInt();
		int insertHigh = dis.readInt();
		int deleteLow = dis.readInt();
		int hwm = dis.readInt();
		boolean seeDeleted = dis.readBoolean();
		return new SpawnIndexedSeqScanReq(handle, insertLow, insertHigh, deleteLow, hwm, seeDeleted);
	}	
}
