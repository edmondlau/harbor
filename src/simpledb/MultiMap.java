//
//  MultiMap.java
//  
//
//  Created by Edmond Lau on 2/3/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb;

import java.util.*;

public class MultiMap<Key, Value> extends HashMap<Key,Set<Value>> {

	public void add(Key k, Value v) {
		Set<Value> values;
		if (containsKey(k)) {
			values = get(k);
		} else {
			values = new HashSet<Value>();
			put(k, values);
		}
		values.add(v);		
	}
	
	public void remove(Key k, Value v) {
		Set<Value> values = get(k);
		values.remove(v);
		if (values.size() == 0)
			remove(k);
	}

}
