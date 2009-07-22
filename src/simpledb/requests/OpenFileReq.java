//
//  OpenFileReq.java
//  
//
//  Created by Edmond Lau on 1/23/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.requests;

import simpledb.*;
import java.io.*;

public class OpenFileReq implements Request, Ackable {
	public String filename;
	
	public OpenFileReq(String filename) {
		this.filename = filename;
	}
	
	public String toString() {
		return "OPEN_FILE_REQ " + filename;
	}
	
	public void serialize(DataOutputStream dos) throws IOException {
		dos.writeInt(OPEN_FILE);
		dos.writeUTF(filename);
	}
	
	public static Request read(DataInputStream dis) throws IOException {
		String filename = dis.readUTF();
		return new OpenFileReq(filename);
	}
}
