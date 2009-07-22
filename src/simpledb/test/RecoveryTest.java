//
//  RecoveryTest.java
//  
//
//  Created by Edmond Lau on 1/26/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.test;

import simpledb.*;
import simpledb.net.*;
import simpledb.requests.*;
import simpledb.version.*;

import java.util.*;


public class RecoveryTest extends Test {

	public boolean runTest(String args[]) {
		int hwm = RecoveryThread.USE_REAL_HWM;
		int size = args.length - 1;
		
		for (int i = 1; i < args.length; i++) {
			if (args[i].equals("-hwm")) {
				hwm = Integer.parseInt(args[i+1]);
				i++;
				size -= 2;
			}
		}

		String[] filenames = new String[size];
		int cursor = 0;
		for (int i = 1; i < args.length; i++) {
			if (args[i].equals("-hwm")) {
				i++;
				continue;
			}
			filenames[cursor++] = args[i];
		}
		
		RecoveryThread[] threads = new RecoveryThread[filenames.length];
		
		Random random = new Random();
		int uniquifier = random.nextInt(50000);		
		for (int i = 0; i < filenames.length; i++) {
			threads[i] = new RecoveryThread(filenames[i], hwm, uniquifier + i*5);
		}
		for (int i = 0; i < threads.length; i++) {
//			threads[i].start();
			threads[i].run();
		}

		return true;	
	}

}
