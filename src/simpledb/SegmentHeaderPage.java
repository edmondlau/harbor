//
//  SegmentHeaderPage.java
//  
//
//  Created by Edmond Lau on 3/1/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//

package simpledb;

import simpledb.version.*;

import java.util.*;
import java.io.*;
import java.text.ParseException;

/**
 * SegmentHeaderPage stores segment-related information regarding a SegmentedHeapFile.
 * It stores triplets of the form
 * <start-epoch, last-deletion-epoch, start-pid> for each segment in text format.
 */
public class SegmentHeaderPage implements Page {

	private PageId pid;
	private boolean dirty;

	// added for logging purposes
	private long pageLsn;

	private List<Segment> segments;
	private File file;
	
	private int totalSize;
	
	// creates a SegmentHeaderPage from the specified file
	public SegmentHeaderPage(PageId id, File data) throws IOException {
		this.pid = id;
		this.file = data;
		segments = new ArrayList<Segment>();
		BufferedReader in = new BufferedReader(new FileReader(data));
		String str;
		
		while ((str = in.readLine()) != null) {
			String[] tokens = str.split(" ");
			Epoch startEpoch = new Epoch(Integer.parseInt(tokens[0]));
			Epoch lastDeletionEpoch = new Epoch(Integer.parseInt(tokens[1]));
			int startPageno = Integer.parseInt(tokens[2]);

			Segment seg = new Segment(startEpoch, lastDeletionEpoch, startPageno);
			segments.add(seg);
		}
	}
	
	public PageId id() {
		return pid;
	}
	
	// returns the segments in this page -- be sure to load the page into the buffer pool first
	public synchronized List<Segment> getSegments() {
		return segments;
	}
	
	public synchronized void write() throws IOException {
		PrintStream printer = new PrintStream(new FileOutputStream(file, false));
		for (Segment seg : segments) {
			printer.println(seg.startEpoch.value() + " " + seg.lastDeletionEpoch.value() + " " + seg.startPageno);
		}
		printer.flush();
		printer.close();
	}
	
	public synchronized long pageLsn() {
		return pageLsn;
	}
	
	public synchronized void setPageLsn(long lsn) {
		pageLsn = lsn;
	}

	public Iterator iterator() {
		return new Iterator<Tuple>() {
			public boolean hasNext() {
				return false;
			}
			
			public Tuple next() {
				throw new NoSuchElementException("no tuples stored in segment header");
			}
			
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};		
	}

	  /**
	   * Marks this page as dirty/not dirty.
	   */
	  public synchronized void markDirty(boolean dirty) {
		// some code goes here
		this.dirty = dirty;
	  }

	  /**
	   * Returns whether or not this page is dirty.
	   */
	  public synchronized boolean isDirty() {
		// some code goes here
		return dirty;
	  }
  }
  
	class Segment {
		Epoch startEpoch;
		Epoch lastDeletionEpoch;
		int startPageno;
		
		public Segment (Epoch startEpoch, Epoch lastDeletionEpoch, int startPageno) {
			this.startEpoch = startEpoch;
			this.lastDeletionEpoch = lastDeletionEpoch;
			this.startPageno = startPageno;
		}
		
		public Segment (Epoch startEpoch, int startPageno) {
			this(startEpoch, new Epoch(0), startPageno);
		}
		
		public String toString() {
			return "<" + startEpoch + "," + lastDeletionEpoch + "," + startPageno + ">";
		}
	}
