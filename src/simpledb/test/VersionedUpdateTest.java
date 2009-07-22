//
//  VersionedUpdateTest.java
//  
//
//  Created by Edmond Lau on 1/30/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.test;

import simpledb.*;
import simpledb.version.*;

import java.util.*;

public class VersionedUpdateTest extends Test {

	public boolean runTest(String args[]) {
		Random rand = new Random(1);
		int numEpochs = Integer.parseInt(args[1]);
		for (int epoch = 1; epoch <= numEpochs; epoch++) {
			TransactionId tid = new TransactionId();
			for (int i = 0; i < 1; i++) {
				SeqScan ss = new SeqScan(tid, file[Integer.parseInt(args[2])-1][0].id());
				
				Predicate p = new Predicate(VersionManager.DELETION_COL, Predicate.EQUALS, IntField.createIntField(0));
				Filter f = new Filter(p, ss);
				
				VersionedUpdate update = new VersionedUpdate(tid, f, 
					new UpdateFunction() {
						public Tuple update(Tuple t) {
							Tuple tuple = new Tuple(t);
							IntField intf = (IntField)tuple.getField(2);
							int nextInt = Integer.parseInt(intf.toString()) + 1;
							tuple.setField(2, IntField.createIntField(nextInt));
							return tuple;
						}
					});
					
				
				try {
					update.open();
					Tuple tuple = update.getNext();
					update.close();
					
					Debug.println("Updated " + tuple + " tuples", 10);
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
			
			VersionManager.Instance().prepare(tid);
			
			if (rand.nextBoolean()) 
				VersionManager.Instance().commit(tid, epoch);
			else
				VersionManager.Instance().abort(tid);
		}
		return true;
	}
}
