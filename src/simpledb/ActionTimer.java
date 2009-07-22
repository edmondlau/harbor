//
//  ActionTimer.java
//  
//
//  Created by Edmond Lau on 2/24/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb;

import java.util.*;

public class ActionTimer {

	public static final Object RECOVERY = new Object();

	private static Map<Object,Long> handlesToStart = new HashMap<Object,Long>();
	
	public static long getTime() {
		return (new Date()).getTime();
	}

	public static void start(Object handle) {
		if (handlesToStart.containsKey(handle))
			throw new RuntimeException("Already have handle: " + handle);
		long start = getTime();
		handlesToStart.put(handle, start);
	}
	
	public static String stopStr(Object handle) {
		long time = stop(handle);
		return timeToString(time);
	}
	
	public static long stop(Object handle) {
		long start = handlesToStart.get(handle);
		handlesToStart.remove(handle);
		return getTime() - start;
	}
	
	public static String timeToString(long ms) {
		double totalSeconds = ms / 1000.0;
		long minutes = ms / (60 * 1000);
		double seconds = totalSeconds - (minutes * 60);
		return minutes + "m" + seconds + "s";
	}

}
