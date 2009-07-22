//
//  OrPredicate.java
//  
//
//  Created by Edmond Lau on 1/30/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb;

public class ConjunctivePredicate extends Predicate {

	private Predicate p1;
	private Predicate p2;
	private int op;

	public static final int AND = 0;
	public static final int OR = 1;

	public Predicate getPred1() { return p1; }
	public Predicate getPred2() { return p2; }
	public int getOp() { return op; }

	public ConjunctivePredicate(int op, Predicate p1, Predicate p2) {
		this.p1 = p1;
		this.p2 = p2;
		this.op = op;
	}
	
	public boolean filter(Tuple t) {
		if (op == AND) {
			return p1.filter(t) && p2.filter(t);
		} else if (op == OR) {
			return p1.filter(t) || p2.filter(t);
		} else {
			throw new RuntimeException("Unrecognized predicate type");
		}
	}
	
	public String toString() {
		String opString = (op == AND) ? "AND" : "OR";
		return "(" + p1 + ") " + opString + " (" + p2 + ")";
	}
}
