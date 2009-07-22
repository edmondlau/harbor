//
//  Project.java
//  
//
//  Created by Edmond Lau on 2/23/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb;

import java.util.*;

public class Project implements DbIterator {

	private TransactionId tid;
	private DbIterator child;
	private int[] columns;
	private TupleDesc td;
	
	public Project(TransactionId tid, DbIterator child, int[] columns) {
		this.tid = tid;
		this.child = child;
		this.columns = new int[columns.length];
		System.arraycopy(columns, 0, this.columns, 0, columns.length);
		// hack -- assumes field types are integers
		TupleDesc childTd = child.getTupleDesc();
		Type[] types = new Type[columns.length];
		for (int i = 0; i < types.length; i++) {
			types[i] = childTd.getType(columns[i]);
		}
		td = new TupleDesc(types);
	}
	
	public TupleDesc getTupleDesc() {
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
	 * Returns tuples projected according to columns.
	 */
	public Tuple getNext() throws NoSuchElementException, TransactionAbortedException {
		Tuple tuple = child.getNext();
		Tuple projectedTuple = new Tuple(td);
		for (int i = 0; i < columns.length; i++) {
			projectedTuple.setField(i, tuple.getField(columns[i]));
		}
		return projectedTuple;
	}
}
