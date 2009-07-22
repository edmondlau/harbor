//
//  SpawnFilterReq.java
//  
//
//  Created by Edmond Lau on 1/26/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.requests;

import simpledb.*;
import java.io.*;

public class SpawnFilterReq extends SpawnOpReq {
	
	public static final int CONJUNCTION = 0;
	public static final int LEAF = 1;
	
	public Predicate pred;
	
	public SpawnFilterReq(long handle, Predicate pred) {
		this.handle = handle;
		this.pred = pred;
	}
	
	public void serializePredicate(Predicate pred, DataOutputStream dos) throws IOException {
		if (pred instanceof ConjunctivePredicate) {
			ConjunctivePredicate conj = (ConjunctivePredicate)pred;
			dos.writeInt(CONJUNCTION);
			dos.writeInt(conj.getOp());
			serializePredicate(conj.getPred1(), dos);
			serializePredicate(conj.getPred2(), dos);
		} else {
			dos.writeInt(LEAF);
			dos.writeInt(pred.getFieldIndex());
			dos.writeInt(pred.getOp());
			IntField intf = (IntField)pred.getOperand();
			dos.writeInt(intf.val());
		}
	}
	
	public void serialize(DataOutputStream dos) throws IOException {
		dos.writeInt(SPAWN_FILTER);
		dos.writeLong(handle);
		serializePredicate(pred, dos);
	}
	
	private static Predicate readPredicate(DataInputStream dis) throws IOException {
		int predType = dis.readInt();
		if (predType == CONJUNCTION) {
			return readConjunctivePredicate(dis);
		} else {
			return readBasicPredicate(dis); 
		}
	}
	
	private static Predicate readConjunctivePredicate(DataInputStream dis) throws IOException {
		int op = dis.readInt();
		Predicate pred1 = readPredicate(dis);
		Predicate pred2 = readPredicate(dis);
		return new ConjunctivePredicate(op, pred1, pred2);
	}
	
	private static Predicate readBasicPredicate(DataInputStream dis) throws IOException {
		int fieldIndex = dis.readInt();
		int op = dis.readInt();
		IntField operand = IntField.createIntField(dis.readInt());
		return new Predicate(fieldIndex, op, operand);
	}
	
	public static Request read(DataInputStream dis) throws IOException {
		long handle = dis.readLong();
		Predicate pred = readPredicate(dis);
		return new SpawnFilterReq(handle, pred);
	}	
	
	public String toString() {
		return "SPAWN_FILTER_REQ " + handle + ", " + pred;
	}
}

