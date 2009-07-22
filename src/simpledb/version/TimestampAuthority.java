//
//  TimestampAuthority.java
//  
//
//  Created by Edmond Lau on 2/16/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.version;

import java.util.*;

public class TimestampAuthority {

	public static int SECONDS_PER_EPOCH = 1;
	private static TimestampAuthority instance = new TimestampAuthority();
	
	public static TimestampAuthority Instance() {
		return instance;
	}
	
	// note that time will wrap around every 2^32 - 1 seconds, which is roughly 126 years
	public Epoch getTime() {
		long seconds = (new Date()).getTime() / 1000;
		int time = (int)(seconds & 0xFFFFFFF);
		return new Epoch(time / SECONDS_PER_EPOCH);
	}

}
