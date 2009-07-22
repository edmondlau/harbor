//
//  VoteReq.java
//  
//
//  Created by Edmond Lau on 1/23/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.requests;

import simpledb.*;
import java.io.*;

public class VoteReq implements Request {
	public static final int YES = 2;
	public static final int NO = 3;
	
	public int vote;
	
	public VoteReq(int vote) {
		this.vote = vote;
	}
	
	public String toString() {
		String v = (vote == YES) ? "YES" : "NO";
		return "VOTE " + v;
	}
	
	public void serialize(DataOutputStream dos) throws IOException {
		dos.writeInt(VOTE);
		dos.writeInt(vote);
	}
	
	public static Request read(DataInputStream dis) throws IOException {
		int vote = dis.readInt();
		return new VoteReq(vote);
	}
}
