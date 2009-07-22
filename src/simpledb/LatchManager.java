//
//  Latch.java
//  
//
//  Created by Edmond Lau on 2/210/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb;

import java.util.*;

/**
 * The LatchManager singleton provides support for latching an object
 * in shared or read-only mode.
 */
public class LatchManager {

	public static final int DEBUG_LATCH = 20;

	private static LatchManager instance = new LatchManager();
	
	private Map<Object,LatchCount> latches;
	
	public LatchManager() {
		latches = new HashMap<Object,LatchCount>();
	}
	
	public static LatchManager Instance() {
		return instance;
	}
	
	public synchronized void latch(Object obj, Permissions perm) {
		Debug.println("Latching " + obj + " with " + perm, DEBUG_LATCH);
		Debug.println(this.toString(), DEBUG_LATCH);
		if (perm.equals(Permissions.EXCLUSIVE)) {
			while (true) {
				if (latches.containsKey(obj)) {
					try {
						wait();
					} catch (InterruptedException e) {
					}
				} else {
					latches.put(obj, new LatchCount(perm, 1));
					break;
				}
			}
		} else {
			while (true) {
				if (!latches.containsKey(obj)) {
					latches.put(obj, new LatchCount(perm, 1));
					break;
				} else {
					LatchCount lc = latches.get(obj);
					if (lc.perm.equals(Permissions.SHARED)) {
						lc.count++;
						break;
					} else {
						try {
							wait();
						} catch (InterruptedException e) {
						}
					}
				}
			}
		}
	}
	
	public synchronized void unlatch(Object obj) {
		assert(latches.containsKey(obj));
		Debug.println("Unlatching " + obj, DEBUG_LATCH);	
		Debug.println(this.toString(), DEBUG_LATCH);
		LatchCount lc = latches.get(obj);
		lc.count--;
		if (lc.count == 0) {
			latches.remove(obj);
		}
		notifyAll();
	}
	
	public String toString() {
		return latches.toString();
	}
	
	class LatchCount {
		public Permissions perm;
		public int count;
		
		public LatchCount(Permissions perm, int count) {
			this.perm = perm;
			this.count = count;
		}
		
		public String toString() {
			return perm.toString() + " " + count;
		}
	}

}
