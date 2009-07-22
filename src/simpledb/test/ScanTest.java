package simpledb.test;

import simpledb.*;
import java.util.*;

/**
 * Dumps the contents of a table.
 * args[1] is the number of columns.  E.g., if it's 5, then ScanTest will end
 * up dumping the contents of f4.txt.
 */
public class ScanTest extends Test {
  public boolean runTest(String args[]) {
    try {
	  if (args[1].equals("segment")) {
		int tableIndex = Integer.parseInt(args[2])-1;
		int insertLow = Integer.parseInt(args[3]);
		int insertHigh = Integer.parseInt(args[4]);
		int deleteLow = Integer.parseInt(args[5]);
		dump(segmentFile[tableIndex], insertLow, insertHigh, deleteLow);
	  } else {
		dump(file[Integer.parseInt(args[1])-1][Integer.parseInt(args[2])]);
	  }
    } catch (TransactionAbortedException te) {
      te.printStackTrace();
      return false;
    }
    return true;
  }
}
