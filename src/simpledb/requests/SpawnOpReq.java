//
//  SpawnOpReq.java
//  
//
//  Created by Edmond Lau on 1/23/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.requests;

public abstract class SpawnOpReq implements Request, Ackable {

	// handle of child db operator to operate upon
	public long handle;
}
