package simpledb;

import java.io.*;
import java.util.*;

/**
 * PageEncoder reads a comma delimited text file and converts it to pages of
 * binary data in the appropriate format for simpledb.
 *
 * Pages are padded out to a specified length, and written consecuctive in a
 * data file.
 */

public class PageEncoder {

  static final int INT_SIZE = 4;
  static final int LONG_SIZE = 8;

   /** Convert the specified input text file into a binary 
    * page file. <br>
    * Assume format of the input file is (note that only integer fields are
    * supported):<br>
    * int,...,int\n<br>
    * int,...,int\n<br>
    * ...<br>
    * where each row represents a tuple.<br>
    * <p>
    * The format of the output file will be as specified in HeapPage and HeapFile.
    * @see HeapPage
    * @see HeapFile
    * @param inFile The input file to read data from
    * @param outFile The output file to write data to
    * @param npagebytes The number of bytes per page in the output file
    * @param numFields the number of fields in each input line/output tuple
    * @throws IOException if the input/output file can't be opened or a malformed input line is encountered
    * 
    */
  public static void convert(File inFile, File outFile, int npagebytes, int numFields) throws IOException {

    int nrecbytes = numFields * INT_SIZE;
    int nrecbits = nrecbytes * 8;
    int nrecords = npagebytes / nrecbytes;

    // per record, we need one bit; there are nrecords per page, so we need
    // nrecords bits, i.e., ((nrecords/32)+1) integers.  
    int nheaderbytes = ((nrecords / 32) + 1) * 4;
    int nheaderbits = nheaderbytes * 8;

    BufferedReader br = new BufferedReader(new FileReader(inFile));
    FileOutputStream os = new FileOutputStream(outFile);

	PrintStream ps = new PrintStream(outFile.getName() + ".seg");

    // our numbers probably won't be much larger than 1024 digits
    char buf[] = new char[1024];

    int curpos = 0;
    int recordcount = 0;
    int npages = 0;

    ByteArrayOutputStream lsnBAOS = new ByteArrayOutputStream(LONG_SIZE);
    DataOutputStream lsnStream = new DataOutputStream(lsnBAOS);
    ByteArrayOutputStream headerBAOS = new ByteArrayOutputStream(nheaderbytes);
    DataOutputStream headerStream = new DataOutputStream(headerBAOS);
    ByteArrayOutputStream pageBAOS = new ByteArrayOutputStream(npagebytes);
    DataOutputStream pageStream = new DataOutputStream(pageBAOS);
	
	int minInsert = Integer.MAX_VALUE;
	int maxDelete = 0;
	
	int fieldCount = 0;

    boolean done = false;
    while(!done) {
      int c = br.read();

      // modified to support windows which represents newlines as '\r\n'
    	if (c == '\r')
    		continue;
    	
      if(c == '\n')
        recordcount++;

      if(c == ',' || c == '\n') {
        String s = new String(buf, 0, curpos);
		int field = Integer.parseInt(s);
		if (fieldCount == 0)
			minInsert = Math.min(minInsert, field);
		if (fieldCount == 1)
			maxDelete = Math.max(maxDelete,field);
		if (c == '\n')
			fieldCount = 0;
		else
			fieldCount++;
        pageStream.writeInt(field);
        curpos = 0;

      } else if (c == -1) {
        done = true;

      } else {
        buf[curpos++] = (char)c;
        continue;
      }

      // if we wrote a full page of records, or if we're done altogether,
      // write out the header of the page.
      //
      // in the header, write a 1 for bits that correspond to records we've
      // written and 0 for empty slots.
      //
      // when we're done, also flush the page to disk, but only if it has
      // records on it.  however, if this file is empty, do flush an empty
      // page to disk.
      if(recordcount >= nrecords || done && recordcount > 0 || done && npages == 0) {
        int i = 0, headerint = 0;

        for(i=0; i<nheaderbits; i++) {
          if(i < recordcount)
            headerint |= (1 << (i % 32));

          if(((i+1) % 32) == 0) {
            headerStream.writeInt(headerint);
            headerint = 0;
          }
        }

        assert(i % 32 == 0);
        if(i % 32 > 0)
          headerStream.writeInt(headerint);

        // pad the rest of the page with zeroes
        for(i=0; i<(npagebytes - recordcount * nrecbytes); i++)
          pageStream.write(0);

		// write the pageLSN
		lsnStream.writeLong(0);
		lsnStream.flush();
		lsnBAOS.writeTo(os);

        // write header and body to file
        headerStream.flush();
        headerBAOS.writeTo(os);
        pageStream.flush();
        pageBAOS.writeTo(os);

        // reset header and body for next page
		lsnBAOS = new ByteArrayOutputStream(LONG_SIZE);
		lsnStream = new DataOutputStream(lsnBAOS);
        headerBAOS = new ByteArrayOutputStream(nheaderbytes);
        headerStream = new DataOutputStream(headerBAOS);
        pageBAOS = new ByteArrayOutputStream(npagebytes);
        pageStream = new DataOutputStream(pageBAOS);

		if (recordcount == 0) {
			minInsert = 0;
		}

        recordcount = 0;
		
        npages++;
		// write a line for the end of every segment
		if (numFields > 2 && npages % SegmentedHeapFile.SEGMENT_SIZE == 0) {
			ps.println(minInsert + " " + maxDelete + " " + (npages - SegmentedHeapFile.SEGMENT_SIZE));
			minInsert = Integer.MAX_VALUE;
			maxDelete = 0;
		}
      }
    }
	
	// check if we need to terminate the last segment
	if (numFields > 2 && npages % SegmentedHeapFile.SEGMENT_SIZE != 0 && SegmentedHeapFile.SEGMENT_SIZE > 1)
		ps.println(minInsert + " " + maxDelete + " " + (npages - (npages % SegmentedHeapFile.SEGMENT_SIZE)));
	ps.close();
    br.close();
    os.close();
  }
}
