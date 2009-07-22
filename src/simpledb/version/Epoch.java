//
//  Epoch.java
//  
//
//  Created by Edmond Lau on 2/27/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.version;

public class Epoch {

	private int time;

	public Epoch(int time) {
		this.time = time;
	}

	public int value() {
		return time;
	}
	
	public String toString() {
		return "" + time;
	}

}
