//
//  VersionedUpdate.java
//  
//
//  Created by Edmond Lau on 1/30/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.version;

import simpledb.*;

import java.util.*;
import java.io.*;

public class VersionedUpdate extends AbstractUpdate {

	private UpdateFunction func;

	public VersionedUpdate(TransactionId tid, DbIterator child, UpdateFunction func) {
		super(tid, child);
		this.func = func;
	}
	
	public void process(Tuple tuple) throws DbException, IOException, TransactionAbortedException {
		VersionManager.Instance().updateTuple(tid, tuple, func);
	}
}
