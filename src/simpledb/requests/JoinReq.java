//
//  JoinReq.java
//  
//
//  Created by Edmond Lau on 2/2/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.requests;

import java.util.*;
import java.io.*;

public class JoinReq implements Request {
	public String hostname;
	public int port;
	
	public List<String> relations;
	
	public JoinReq(String hostname, int port, List<String> relations) {
		this.hostname = hostname;
		this.port = port;
		this.relations = new ArrayList<String>(relations);
	}
	
	public void serialize(DataOutputStream dos) throws IOException {
		dos.writeInt(JOIN);
		dos.writeUTF(hostname);
		dos.writeInt(port);
		dos.writeInt(relations.size());
		for (int i = 0; i < relations.size(); i++)
			dos.writeUTF(relations.get(i));
	}
	
	public static Request read(DataInputStream dis) throws IOException {
		String hostname = dis.readUTF();
		int port = dis.readInt();
		int size = dis.readInt();
		List<String> relations = new ArrayList<String>();
		for (int i = 0; i < size; i++)
			relations.add(dis.readUTF());
		return new JoinReq(hostname, port, relations);
	}
}
