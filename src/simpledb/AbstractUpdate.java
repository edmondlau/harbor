//
//  AbstractUpdate.java
//  
//
//  Created by Edmond Lau on 2/16/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb;

import java.util.*;
import java.io.*;

public abstract class AbstractUpdate implements DbIterator {

	protected TransactionId tid;
	protected DbIterator child;
	
	private TupleDesc td;
	private boolean updateCompleted;

	public AbstractUpdate(TransactionId tid, DbIterator child) {
		this.tid = tid;
		this.child = child;
	}
	
	public TupleDesc getTupleDesc() {
		if (td == null) {
			Type type[] = new Type[1];
			type[0] = Type.INT_TYPE;
			td = new TupleDesc(type);
		}
		return td;
	}

	public void open() throws DbException, TransactionAbortedException {
		child.open();
	}

	public void close() {
		child.close();
	}

	public void rewind() throws DbException, TransactionAbortedException {
		child.rewind();
	}
	
	/**
	 * DbIterator getNext method.
	 * Updates tuples as they are read from the child operator.  Updates are
	 * processed via the VersionManager.
	 * @ return a 1-field tuple containing the number of updated recoreds.
	 */
	public Tuple getNext() throws NoSuchElementException, TransactionAbortedException {
	  	if (updateCompleted) {
			throw new NoSuchElementException("Completed update");
		}
		int count = 0;
		try {
			while (true) {
				Tuple tuple = child.getNext();
				try {
					process(tuple);
					count++;
				} catch (DbException de) {
					de.printStackTrace();
					throw new RuntimeException("Failed to update a tuple");
				} catch (IOException ioe) {
					ioe.printStackTrace();
					throw new RuntimeException("Failed to update a tuple");
				}
			}
		} catch (NoSuchElementException noee) { 
			// no more tuples to delete
		}
		updateCompleted = true;
		Tuple result = new Tuple(getTupleDesc());
		result.setField(0, IntField.createIntField(count));
		return result;
	}

	public abstract void process(Tuple tuple) throws DbException, IOException, TransactionAbortedException;
}
