//
//  MultiInsertTest.java
//  
//
//  Created by Edmond Lau on 2/28/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.test;

import simpledb.*;
import java.util.*;
import java.io.*;

public class MultiInsertTest extends InsertTest {

	public boolean runTest(String args[]) {
		int n = Integer.parseInt(args[4]);
		
		for (int i = 0; i < n; i++) {
			super.runTest(args);
		}
		return true;
	}

}
