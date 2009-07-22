package simpledb;

import java.util.*;
import java.io.*;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 */

public class Catalog {

  private final static Catalog _instance = new Catalog();
  private Map<Integer, DbFile> tidsToDbFiles; /* int -> DbFile */
  private Map<Integer, TupleDesc> tidsToTupleDescs; /* int -> TupleDesc */
  
  private Map<String, Integer> filesToTableIds; /* File -> int */
  
  private int tableId = 0;
  /**
   * Constructor.
   * Creates a new, empty catalog.
   */
  private Catalog() {
    // some code goes here
  	tidsToDbFiles = new HashMap<Integer, DbFile>();
  	tidsToTupleDescs = new HashMap<Integer, TupleDesc>();
  	filesToTableIds = new HashMap<String, Integer>();
  }

  /**
   * Access to Catalog instance.
   */
  public static Catalog Instance() {
    return _instance;
  }
  
  public synchronized int getTableId(String filename) {
   	if (filesToTableIds.containsKey(filename)) {
  		return filesToTableIds.get(filename);
  	}
  	filesToTableIds.put(filename, tableId);
  	return tableId++; 
  }
  
  public synchronized int getTableId(File file) {
	return getTableId(file.getName());
  }

  /**
   * Add a new table to the catalog.
   * This table has tuples formatted using the specified TupleDesc and its
   * contents are stored in the specified DbFile. 
   * @param file the contents of the table to add
   * @param t the format of tuples that are being added
   */
  public synchronized void addTable(DbFile file, TupleDesc t) {
    // some code goes here
  	tidsToDbFiles.put(file.id(), file);
  	tidsToTupleDescs.put(file.id(), t);
  }

  /**
   * Returns the tuple descriptor (schema) of the specified table
   */
  public synchronized TupleDesc getTupleDesc(int tableid) throws NoSuchElementException{
    // some code goes here
  	if (tidsToTupleDescs.containsKey(tableid)) {
  		return tidsToTupleDescs.get(tableid);
  	}
  	throw new NoSuchElementException("TupleDesc for table id " + tableid + " not found");
  }

  /**
   * Returns the DbFile that can be used to read the contents of the specified table.
   */
  public synchronized DbFile getDbFile(int tableid) throws NoSuchElementException {
    // some code goes here
  	if (tidsToDbFiles.containsKey(tableid)) {
  		return tidsToDbFiles.get(tableid);
  	}
  	throw new NoSuchElementException("DbFile for table id " + tableid + " not found");
  }
  
  public synchronized DbFile getDbFile(String filename) throws NoSuchElementException {
	return getDbFile(getTableId(filename));
  }
}
