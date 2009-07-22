/**
 * Created on Jan 23, 2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package simpledb.requests;

import java.io.*;

public interface Request {
	
	public static final int BEGIN = 0;
	public static final int ABORT = 1;
	public static final int COMMIT = 2;
	public static final int PREPARE = 3;
	
	public static final int ACQUIRE_TABLE_LOCK = 4;
	public static final int GRANT_LOCK_ACK = 5;
	public static final int RELEASE_TABLE_LOCK = 6;
	
	public static final int OPEN_FILE = 7;
	public static final int SPAWN_DELETE = 8;
	public static final int SPAWN_FILTER = 9;
	public static final int SPAWN_VERSIONED_INSERT = 10;
	public static final int SPAWN_NET_INSERT = 11;
	public static final int SPAWN_NET_SCAN = 12;
	public static final int SPAWN_SEQ_SCAN = 13;
	public static final int SPAWN_VERSIONED_SEQ_SCAN = 14;
	public static final int SPAWN_VERSIONED_UPDATE = 15;
	
	public static final int VOTE = 16;
	public static final int JOIN = 17;
	public static final int EXECUTE = 18;
	
	public static final int OP_ACK = 19;
	
	public static final int BATCH = 21;
	public static final int SPAWN_PROJECT = 22;
	public static final int SPAWN_INDEXED_SEQ_SCAN = 23;
	public static final int FLUSH = 24;
	
	public static final int PREPARE_TO_COMMIT = 25;
	
	// writes this request to the dos
	public void serialize(DataOutputStream dos) throws IOException;
	
	// initializes this request based on incoming data
//	public static Request read(DataInputStream dis) throws IOException;
}
