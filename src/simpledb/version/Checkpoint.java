//
//  Checkpoint.java
//  
//
//  Created by Edmond Lau on 2/23/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.version;

import java.io.*;

public class Checkpoint {
	
	private String filename;
	
	public static Checkpoint ARIES_CHKPT = new Checkpoint("aries-chkpt");
	public static Checkpoint VERSIONED_CHKPT = new Checkpoint("versioned-chkpt");
	
	private Checkpoint(String filename) {
		this.filename = filename;
	}
	
	public void write(long checkpoint) {
		try {
			PrintStream ps = new PrintStream(new File(filename));
			ps.print(checkpoint);
			ps.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
			System.exit(-1);
		}
	}
	
	public long read() {
		try {
			BufferedReader in = new BufferedReader(new FileReader(filename));
			String str;
			str = in.readLine();
			if (str == null)
				return 0;
			else
				return Long.parseLong(str);
		} catch (IOException ioe) {
			System.out.println("No checkpoint found: returning 0");
			return 0;
		}	
	}

}
