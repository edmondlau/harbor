//
//  DeleteFunction.java
//  
//
//  Created by Edmond Lau on 2/28/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.version;

import simpledb.*;

import java.io.*;

public interface DeleteFunction extends UpdateFunction {
	
	public Epoch getDeletionEpoch();
}