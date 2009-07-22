//
//  BatchReq.java
//  
//
//  Created by Edmond Lau on 2/21/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.requests;

import java.io.*;

/**
 * The convention is that any handles in the batch requests
 * point to indices of previous requests and will be substituted
 * in later.
 */
public class BatchReq implements Request {

	public Request[] requests;

	public BatchReq(Request[] reqs) {
		requests = new Request[reqs.length];
		for (int i = 0; i < reqs.length; i++)
			requests[i] = reqs[i];
	}
	
	public void serialize(DataOutputStream dos) throws IOException {
		dos.writeInt(BATCH);
		dos.writeInt(requests.length);
		for (int i = 0; i < requests.length; i++)
			requests[i].serialize(dos);
	}
	
	public static Request read(DataInputStream dis) throws IOException {
		int length = dis.readInt();
		Request[] reqs = new Request[length];
		for (int i = 0; i < reqs.length; i++) {
			reqs[i] = RequestDispatcher.read(dis);
		}
		return new BatchReq(reqs);
	}
	
	public String toString() {
		StringBuffer buf = new StringBuffer();
		for (int i = 0; i < requests.length; i++) {
			buf.append("\t" + requests[i]);
			if (i != requests.length - 1)
				buf.append("\n");
		}
		return "BATCH_REQ \n" + buf.toString();
	}
	
}
