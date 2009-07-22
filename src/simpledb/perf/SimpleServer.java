//
//  SimpleServer.java
//  
//
//  Created by Edmond Lau on 3/4/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.perf;

import java.net.*;
import java.io.*;
import java.util.*;

public class SimpleServer {

	public static final int PORT = 50000;

	public void dispatch() {
		ServerSocket server = null;
		
		try {
			server = new ServerSocket(PORT);
			server.setReceiveBufferSize(1000000);
			while (true) {
				Socket socket = server.accept();
				socket.setTcpNoDelay(true);	
//				socket.setReceiveBufferSize(1000000);
				socket.setSendBufferSize(1000000);			
				System.out.println("Send Buffer size: " + socket.getSendBufferSize());
				System.out.println("Receive Buffer size: " + socket.getReceiveBufferSize());
				DataInputStream dis = null;
				DataOutputStream dos = null;
				
				try {
					dis = new DataInputStream(socket.getInputStream());		
					dos = new DataOutputStream(socket.getOutputStream());
					// if a the client closes the connection, we'll get an
					// EOF exception and exit the while loop					
					while (true) {
						int n = 0;
						
						for (int i = 0; i < SimpleClient.DATA_SIZE; i++)
							 n = dis.readInt();
						System.out.println("Read " + n);
										
						dos.writeInt(n);
						dis.readInt();
					}
				} catch(IOException ioe) {
					dis.close();
					dos.close();
					socket.close();
				}
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
			System.exit(-1);
		} finally {
			try {
				if (server != null) {
					server.close();
				}
			} catch (IOException ioe) {}
		}
	}
	
	public static void main(String[] args) {
		(new SimpleServer()).dispatch();
	}
}
