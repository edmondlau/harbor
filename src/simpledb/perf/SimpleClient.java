//
//  SimpleClient.java
//  
//
//  Created by Edmond Lau on 3/4/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.perf;

import java.net.*;
import java.io.*;
import java.util.*;

public class SimpleClient {

	private boolean reuse;
	private int curVal;
	private Socket socket;
	private String host = "north-wind.lcs.mit.edu";
	
	public static final int DATA_SIZE = 16;
	
	private int nTrials;

	public SimpleClient (int trials, boolean recycle) {
		reuse = recycle;
		nTrials = trials;
		curVal = 0;
	}

	public void run() {
		try {
			if (reuse) {
				// make one socket connection for all trials
				Socket socket = new Socket();
				socket.setTcpNoDelay(true);
				socket.setReceiveBufferSize(1000000);
				socket.setSendBufferSize(1000000);				
				socket.bind(null);
				socket.connect(new InetSocketAddress(host, SimpleServer.PORT));
//				Socket socket = new Socket(host, SimpleServer.PORT);

				System.out.println("Send Buffer size: " + socket.getSendBufferSize());
				System.out.println("Receive Buffer size: " + socket.getReceiveBufferSize());				
				DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
				DataInputStream dis = new DataInputStream(socket.getInputStream());	
				for (int j = 0; j < nTrials; j++) {
					for (int i = 0; i < DATA_SIZE; i++) {
						dos.writeInt(curVal);
					}
					curVal++;
					dos.flush();
					int n = dis.readInt();
					System.out.println("Read " + n);
					dos.writeInt(n);
				}
				dis.close();
				dos.close();
				socket.close();
			} else {
				// for each trial, create a new socket connection
				for (int j = 0; j < nTrials; j++) {
					Socket socket = new Socket(host, SimpleServer.PORT);
					socket.setTcpNoDelay(true);
					socket.setReceiveBufferSize(1000000);
					socket.setSendBufferSize(1000000);					
					DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
					DataInputStream dis = new DataInputStream(socket.getInputStream());						
					
					for (int i = 0; i < DATA_SIZE; i++) {
						dos.writeInt(curVal);
					}
					curVal++;
					
					int n = dis.readInt();
					System.out.println("Read " + n);
					dos.writeInt(n);			
					dis.close();
					dos.close();
					socket.close();	
				}
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		int nTrials = Integer.parseInt(args[0]);
		if (args[1].equals("reuse")) {
			(new SimpleClient(nTrials, true)).run();
		} else {
			(new SimpleClient(nTrials, false)).run();			
		}
	}
}
