package simpledb;

import java.io.*;
import java.util.*;
import java.text.ParseException;

import simpledb.log.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	// PageId = <tableid, pgNo> -- needs to know which pageids exists
	protected int tableId;
	protected File file;
	protected int firstPageWithSlot;
	
	private long memLength; // file's on-disk length + size of any in-memory pages

	/**
	 * Constructor. Creates a new heap file with the specified tuple descriptor
	 * that stores pages in the specified buffer pool.
	 * 
	 * @param f
	 *          The file that stores the on-disk backing store for this DbFile.
	 */
	public HeapFile(File f) {
		// some code goes here
		tableId = Catalog.Instance().getTableId(f);
		file = f;
		firstPageWithSlot = -1;
		memLength = -1;
	}

	/**
	 */
	public int id() {
		// some code goes here
		return tableId;
	}

	/**
	 * Returns a Page from the file.
	 */
	public Page readPage(PageId id) throws NoSuchElementException {
		// some code goes here
		if (id.tableid() != tableId) {
			throw new RuntimeException("Looking for table id " + id.tableid()
					+ " but found table id " + tableId);
		}

		// pageSize is header size + BufferPool.PAGE_SIZE + pageLsn size
		int pageSize = totalPageSize();

		long offset = (long)id.pageno() * (long)pageSize;
		if (offset >= file.length() && offset < memLength) {
			try {
				// create a new page on the fly
				byte[] data = HeapPage.createEmptyPageData(tableId);
				HeapPage newPage = new HeapPage(id, data);
				return newPage;
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("Error creating new page: " + id);
			}
		}

		byte[] data = new byte[pageSize];
		int bytesRead;

		try {
			RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
			if (offset > 0)
				randomAccessFile.seek(offset);
			bytesRead = randomAccessFile.read(data);
			randomAccessFile.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
			throw new RuntimeException("IO problem reading file " + file);
		}

		if (bytesRead < pageSize) {
			throw new RuntimeException("Tried to read " + pageSize
					+ " bytes but only read " + bytesRead + " bytes");
		}

		HeapPage page = null;
		try {
			page = new HeapPage(id, data);
		} catch (ParseException pe) {
			pe.printStackTrace();
			throw new RuntimeException("Parse problem");
		} catch (IOException ioe) {
			ioe.printStackTrace();
			throw new RuntimeException("IO problem");
		}
		return page;
	}

	/**
	 * Writes the given page to the appropriate location in the file.
	 */
	public void writePage(Page p) throws IOException {
		// some code goes here
		PageId id = p.id();
		HeapPage heapPage = (HeapPage) p;

		if (id.tableid() != tableId) {
			throw new IOException("Writing for table id " + id.tableid()
					+ " but found table id " + tableId);
		}

		int pageSize = totalPageSize();
		RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
		// the page we're looking for is at id.pageno() * (header + page bytes)
		long offset = (long)id.pageno() * (long)pageSize;
		randomAccessFile.seek(offset);
		randomAccessFile.write(heapPage.getPageData());
		randomAccessFile.close();
	}

	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages() {
		if (memLength == -1)
			memLength = file.length();
		// some code goes here
		long pageSize = (long) totalPageSize();
		assert (file.length() % pageSize == 0);
//		return (int) (file.length() / pageSize);
		return (int) (memLength / pageSize);
	}

	/**
	 * Adds the specified tuple to the table under the specified TransactionId.
	 * 
	 * @return RecordId the RecordId assigned to the new tuple
	 * @throws DbException
	 * @throws IOException
	 */
	public RecordId addTuple(TransactionId tid, Tuple t) throws DbException,
			IOException, TransactionAbortedException {
		if (firstPageWithSlot == -1)
			firstPageWithSlot = Math.max(0, numPages() - 1);
		// some code goes here
		// use the last modified page as a hint of where to insert
		for (int i = firstPageWithSlot; i < numPages(); i++) {
			PageId pid = new PageId(tableId, i);
			Permissions oldPerms = LockManager.Instance().lookupAccess(tid, pid);
			
			HeapPage page = (HeapPage)BufferPool.Instance().getPage(tid, pid, Permissions.READ_ONLY);
			if (page.getNumEmptySlots() > 0) {
				page = (HeapPage)BufferPool.Instance().getPage(tid, pid, Permissions.READ_WRITE);
				RecordId rid = page.addTuple(t);
				
				firstPageWithSlot = i;
				
				if (BufferPool.Instance().loggingOn()) {
					page.setPageLsn(Log.Instance().logInsert(tid, rid, t));
				}				
				return rid;
			} else {
				if (oldPerms == null)
					// release the lock on the page b/c we don't need it
					BufferPool.Instance().releasePage(tid, pid);
			}
		}
		
		// no more space in current heap pages, so make a new one
//		try {
			// create a blank page
			int numPages = numPages();
			PageId pid = new PageId(tableId, numPages);
			firstPageWithSlot = numPages;
//			byte[] data = HeapPage.createEmptyPageData(tableId);
//			HeapPage newPage = new HeapPage(pid, data);
			// XXX write this new page out to disk?
			memLength += totalPageSize();
//			writePage(newPage);
			// now load the page into the buffer pool and add the new tuple
			HeapPage page = (HeapPage)BufferPool.Instance().getPage(tid, pid, Permissions.READ_WRITE);
			RecordId rid = page.addTuple(t);
			if (BufferPool.Instance().loggingOn()) {
				page.setPageLsn(Log.Instance().logInsert(tid, rid, t));
			}
			return rid;
//		} catch (ParseException pe) {
//			throw new DbException("Unable to add tuple to new heap page of heap file " + tableId);
//		}
	}

	/**
	 * Deletes the specified tuple from the table, under the specified
	 * TransactionId.
	 * @return Page from which the tuple was deleted
	 */
	public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
			TransactionAbortedException {
			
		if (firstPageWithSlot == -1)
			firstPageWithSlot = numPages() - 1;
						
		// some code goes here
		RecordId rid = t.getRecordId();
		PageId pid = new PageId(rid.tableid(), rid.pageno());
		HeapPage page = (HeapPage)BufferPool.Instance().getPage(tid, pid, Permissions.READ_WRITE);
		
		page.deleteTuple(t);
		if (pid.pageno() < firstPageWithSlot) {
			firstPageWithSlot = pid.pageno();
		}
		
		if (BufferPool.Instance().loggingOn()) {
			page.setPageLsn(Log.Instance().logDelete(tid, rid, t));	
		}
		return page;
	}
	
	public Page updateTuple(TransactionId tid, RecordId rid, UpdateFunction func) throws DbException, TransactionAbortedException {
		PageId pid = new PageId(rid.tableid(), rid.pageno());
		HeapPage page = (HeapPage)BufferPool.Instance().getPage(tid, pid, Permissions.READ_WRITE);
		
		Tuple oldTuple = page.updateTuple(rid, func);
		if (BufferPool.Instance().loggingOn()) {
			page.setPageLsn(Log.Instance().logUpdate(tid, rid, oldTuple, func.update(oldTuple)));
		}
		return page;
	}

	protected int totalPageSize() {
		// look up tuple description from the catalog
		// client must have added tuple description to catalog in order to read a
		// page from DbFile
		TupleDesc td = Catalog.Instance().getTupleDesc(tableId);

		// calculate the size of a header for this DbFile
		int numSlotsPerPage = BufferPool.PAGE_SIZE / td.getSize();
		int numHeadersPerPage = numSlotsPerPage / 32 + 1;
		int numHeaderBytes = numHeadersPerPage * 4;
		int pageSize = numHeaderBytes + BufferPool.PAGE_SIZE + 8; // 8 for pageLSN

		return pageSize;
	}

}
