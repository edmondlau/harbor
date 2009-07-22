//
//  CommLayer.java
//  
//
//  Created by Edmond Lau on 1/24/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.net;

import simpledb.*;
import simpledb.requests.*;
import static simpledb.requests.Request.*;

import java.net.*;
import java.io.*;

/**
 * A CommLayer is a wrapper around a socket that provides basic
 * communication primitives with blocking I/O.
 */

public class CommLayer {
	
	// socket variables
	private Socket socket;
	private DataOutputStream out;
	private DataInputStream in;
	
	// server or client
	public static final boolean SERVER = true;
	public static final boolean CLIENT = false;
	
	// creates a CommLayer wrapper object out of an already open connection
	public CommLayer(Socket socket, boolean server) {
		assert(socket.isConnected());
		
		this.socket = socket;
		try {
			if (server == CLIENT) {
				in = new DataInputStream(socket.getInputStream());
				out = new DataOutputStream(socket.getOutputStream());
			} else {
				out = new DataOutputStream(socket.getOutputStream());
				in = new DataInputStream(socket.getInputStream());
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
			System.exit(-1);
		}
	}
	
	public Socket getSocket() {
		return socket;
	}

	public Request read() throws EOFException {
		// read int to determine type
		try {
			return RequestDispatcher.read(in);
		} catch (EOFException eof) {
			throw eof;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public void write(Request req) throws EOFException {
		Debug.println("COMM Sending " + req, 5);
		try {
			req.serialize(out);
			out.flush();
		} catch (IOException ioe) {
			System.out.println("IOException at CommLayer.write: " + ioe.getMessage());
			ioe.printStackTrace();
		}
	}

//	public InputType read() throws EOFException {
//		InputType obj = null;
//		try {
//			obj = (InputType)in.readObject();
//		} catch (EOFException eof) {
//			throw eof;
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		Debug.println("Receiving " + obj, 5);
//		return obj;
//	}
//	
//	public void write(OutputType msg) {
//		Debug.println("Sending " + msg, 5);
//		try {
//			out.writeObject(msg);
//		} catch (IOException ioe) {
//			System.out.println("IOException at CommLayer.write: " + ioe.getMessage());
//			ioe.printStackTrace();
//		}
//	}
	
	public void close() {
		try {
			socket.shutdownInput();
			in.close();		
			out.close();
			socket.close();
		} catch (IOException ioe) {
//			System.out.println("IOException at CommLayer.close: " + ioe.getMessage());
			ioe.printStackTrace();
		}
	}

}
