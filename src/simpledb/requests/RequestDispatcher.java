//
//  RequestDispatcher.java
//  
//
//  Created by Edmond Lau on 2/21/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.requests;

import static simpledb.requests.Request.*;
import java.io.*;

public class RequestDispatcher {

	public static Request read(DataInputStream in) throws IOException {
		int requestType = in.readInt();
		switch (requestType) {
			case BEGIN:
				return BeginReq.read(in);
			case ABORT:
				return AbortReq.read(in);
			case COMMIT:
				return CommitReq.read(in);
			case PREPARE:
				return PrepareReq.read(in);
			case ACQUIRE_TABLE_LOCK:
				return AcquireTableLockReq.read(in);
			case GRANT_LOCK_ACK:
				return GrantLockAck.read(in);
			case RELEASE_TABLE_LOCK:
				return ReleaseTableLockReq.read(in);
			case OPEN_FILE:
				return OpenFileReq.read(in);
			case SPAWN_DELETE:
				return SpawnDeleteReq.read(in);
			case SPAWN_FILTER:
				return SpawnFilterReq.read(in);
			case SPAWN_VERSIONED_INSERT:
				return SpawnVersionedInsertReq.read(in);
			case SPAWN_NET_INSERT:
				return SpawnNetInsertReq.read(in);
			case SPAWN_NET_SCAN:
				return SpawnNetScanReq.read(in);
			case SPAWN_SEQ_SCAN:
				return SpawnSeqScanReq.read(in);
			case SPAWN_VERSIONED_SEQ_SCAN:
				return SpawnVersionedSeqScanReq.read(in);
			case SPAWN_VERSIONED_UPDATE:
				return SpawnVersionedUpdateReq.read(in);
			case VOTE:
				return VoteReq.read(in);
			case JOIN:
				return JoinReq.read(in);
			case EXECUTE:
				return ExecuteReq.read(in);
			case OP_ACK:
				return OpAck.read(in);
			case BATCH:
				return BatchReq.read(in);
			case SPAWN_PROJECT:
				return SpawnProjectReq.read(in);
			case SPAWN_INDEXED_SEQ_SCAN:
				return SpawnIndexedSeqScanReq.read(in);
			case FLUSH:
				return FlushReq.read(in);
			case PREPARE_TO_COMMIT:
				return PrepareToCommitReq.read(in);
			default:
				throw new RuntimeException("Unrecognized request type: " + requestType);	
		}
	}

}
