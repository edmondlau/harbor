//
//  SegmentedHeapFile.java
//  
//
//  Created by Edmond Lau on 3/1/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb;

import java.io.*;
import java.util.*;
import java.text.ParseException;

import simpledb.log.*;
import simpledb.version.*;

public class SegmentedHeapFile extends HeapFile {

	public static final int SEGMENT_SIZE = 2560; // 10MB // number of pages

	private PageId headerId;
	private String prefix;

	public SegmentedHeapFile(File f) {
		super(f);
		headerId = new PageId(tableId, -1);
	}
	
	public PageId getHeaderId() {
		return headerId;
	}
	
	public int id() {
		return tableId;
	}

	public Page readPage(PageId id) throws NoSuchElementException {
		if (!id.equals(headerId)) {
			return super.readPage(id);
		}
		
		SegmentHeaderPage page;
		
		try {
			page = new SegmentHeaderPage(id, new File(file.getName() + ".seg"));
		} catch (IOException ioe) {
			ioe.printStackTrace();
			throw new RuntimeException("IO problem");
		} 
		return page;
	}

	public void writePage(Page p) throws IOException {
		if (!p.id().equals(headerId)) {
			super.writePage(p);
			return;
		}
		
		// some code goes here
		PageId id = p.id();
		SegmentHeaderPage header = (SegmentHeaderPage) p;

		if (id.tableid() != tableId) {
			throw new IOException("Writing for table id " + id.tableid()
					+ " but found table id " + tableId);
		}

		header.write();
	}
	
//	private boolean isLastSegmentFull(TransactionId tid, List<Segment> segs) throws TransactionAbortedException {
//		if (numPages() - segs.get(segs.size() - 1).startPageno < SEGMENT_SIZE)
//			return false;
//
//		HeapPage page = (HeapPage)BufferPool.Instance().getPage(tid, new PageId(tableId, numPages() - 1), Permissions.READ_ONLY);
//		return page.getNumEmptySlots() == 0;		
//	}
//	
//	public RecordId addTuple(TransactionId tid, Tuple t) throws DbException,
//			IOException, TransactionAbortedException {
//			
//		SegmentHeaderPage page = (SegmentHeaderPage)BufferPool.Instance().getPage(tid, headerId, Permissions.READ_ONLY);
//		List<Segment> segs = page.getSegments();
//									
//		if (segs.size() == 0 || isLastSegmentFull(tid, segs)) {
//			page = (SegmentHeaderPage)BufferPool.Instance().getPage(tid, headerId, Permissions.READ_WRITE);
//			Segment seg = new Segment(TimestampAuthority.Instance().getTime(), numPages());
//			segs.add(seg);
//			page.markDirty(true);	
//		}
//			
//		return super.addTuple(tid, t);
//	}
	
	public boolean isLastSegmentFull(TransactionId tid) throws TransactionAbortedException {
		if (numPages() % SEGMENT_SIZE != 0)
			return false;
			
		SegmentHeaderPage header = (SegmentHeaderPage)BufferPool.Instance().getPage(tid, headerId, Permissions.READ_ONLY);
		List<Segment> segs = header.getSegments();
					
		// check again to prevent diarrhea of segments if we're missing a chunk in the middle
		if (numPages() - segs.get(segs.size() - 1).startPageno < SEGMENT_SIZE)
			return false;			
		
		HeapPage page = (HeapPage)BufferPool.Instance().getPage(tid, new PageId(tableId, numPages() - 1), Permissions.READ_ONLY);
		return page.getNumEmptySlots() == 0;
	}
	
	public RecordId addTuple(TransactionId tid, Tuple t) throws DbException,
			IOException, TransactionAbortedException {
									
		if (numPages() == 0 || isLastSegmentFull(tid)) {
			SegmentHeaderPage page = (SegmentHeaderPage)BufferPool.Instance().getPage(tid, headerId, Permissions.READ_WRITE);
			List<Segment> segs = page.getSegments();
			if (!(numPages() == 0 && segs.size() != 0)) { // handle the case where there are no heap pages but there is a segment
				Segment seg = new Segment(TimestampAuthority.Instance().getTime(), numPages());
				page.getSegments().add(seg);
				page.markDirty(true);
			}
		}
			
		return super.addTuple(tid, t);
	}		
	
	public Page updateTuple(TransactionId tid, RecordId rid, UpdateFunction func) throws DbException, TransactionAbortedException {
		if (func instanceof DeleteFunction) {
			DeleteFunction deleteFunc = (DeleteFunction)func;
			
			SegmentHeaderPage page = (SegmentHeaderPage)BufferPool.Instance().getPage(tid, headerId, Permissions.READ_WRITE);
			List<Segment> segments = page.getSegments();			
			
			boolean found = false;
			
			for (int i = 0; i < segments.size() - 1; i++) {
				Segment s1 = segments.get(i);
				Segment s2 = segments.get(i+1);
				
				if (rid.pageno() >= s1.startPageno && rid.pageno() < s2.startPageno) {
					s1.lastDeletionEpoch = deleteFunc.getDeletionEpoch();
					found = true;
					break;
				}
			}
			if (!found)
				segments.get(segments.size()-1).lastDeletionEpoch = deleteFunc.getDeletionEpoch();
			page.markDirty(true);		
		}
		
		return super.updateTuple(tid, rid, func);
	}
	
//	public int firstPageOnSegmentWith(TransactionId tid, Epoch time) throws TransactionAbortedException {
//		SegmentHeaderPage page = (SegmentHeaderPage)BufferPool.Instance().getPage(tid, headerId, Permissions.READ_ONLY);
//		List<Segment> segments = page.getSegments();
//		System.out.println(segments);
//		
//		if (segments.size() > 0 && time.value() < segments.get(0).startEpoch.value())
//			return segments.get(0).startPageno;
//	
//		for (int i = 0; i < segments.size() - 1; i++) {
//			Segment s1 = segments.get(i);
//			Segment s2 = segments.get(i+1);
//			
//			if (s1.startEpoch.value() <= time.value() && time.value() < s2.startEpoch.value()) {
//				return s1.startPageno;
//			}
//		}
//		return segments.get(segments.size()-1).startPageno;
//	}
//
//	// includes page to scan?
//	public int lastPageOnSegmentWith(TransactionId tid, Epoch time) throws TransactionAbortedException {
//		SegmentHeaderPage page = (SegmentHeaderPage)BufferPool.Instance().getPage(tid, headerId, Permissions.READ_ONLY);
//		List<Segment> segments = page.getSegments();	
//		
//		if (segments.size() > 0 && time.value() < segments.get(0).startEpoch.value())
//			return -1;
//		
//		for (int i = 0; i < segments.size() - 1; i++) {
//			Segment s1 = segments.get(i);
//			Segment s2 = segments.get(i+1);
//			
//			if (s1.startEpoch.value() <= time.value() && time.value() < s2.startEpoch.value()) {
//				return s2.startPageno - 1;
//			}
//		}
//		return numPages() - 1;
//	}
}
