//
//  Update.java
//  
//
//  Created by Edmond Lau on 2/23/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb;
import java.util.*;
import java.io.*;

public class Update extends AbstractUpdate {
	private UpdateFunction func;
	
	// for each tuple in child, set it to func(child)
	public Update(TransactionId t, DbIterator child, UpdateFunction func) {
		super(t, child);
		this.func = func;
	}
	
	public void process(Tuple tuple) throws IOException, DbException, TransactionAbortedException {
		BufferPool.Instance().updateTuple(tid, tuple.getRecordId(), func);
	}
	
}
