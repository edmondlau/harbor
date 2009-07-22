//
//  VersionedDelete.java
//  
//
//  Created by Edmond Lau on 2/16/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.version;

import simpledb.*;

import java.util.*;
import java.io.*;

public class VersionedDelete extends AbstractUpdate {

	private int tableId;

	public VersionedDelete(TransactionId tid, DbIterator child) {
		super(tid, child);
	}
	
	public void process(Tuple tuple) throws DbException, IOException, TransactionAbortedException {
		VersionManager.Instance().deleteTuple(tid, tuple);
	}
}

