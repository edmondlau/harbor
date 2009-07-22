//
//  VersionedInsert.java
//  
//
//  Created by Edmond Lau on 2/16/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.version;

import simpledb.*;

import java.util.*;
import java.io.*;

public class VersionedInsert extends AbstractUpdate {

	private int tableid;

	public VersionedInsert(TransactionId tid, DbIterator child, int tableid) {
		super(tid, child);
		this.tableid = tableid;
	}
	
	public void process(Tuple tuple) throws DbException, IOException, TransactionAbortedException {
		VersionManager.Instance().insertTuple(tid, tableid, tuple);
	}
}

