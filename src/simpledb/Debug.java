package simpledb;

/** 

 Debug is a utility class that wraps println statements and allows
 more or less command line output to be turned on.

 <p> 

 The idea is to change the value of the DEBUG_LEVEL constant at
 compile time, then call println(s,level) with a level number.  If
 the level number is less than or equal to the currently set
 DEBUG_LEVEL, the system will print the string; otherwise, output will
 be supressed.

*/

public class Debug {
  public static final int DEBUG_LEVEL = 0;
  public static final int DEFAULT_LEVEL = 1;

  public static void println(String s, int level) {
    if (level <= DEBUG_LEVEL)
      System.out.println(s);
  }

  public static boolean level(int level) {
    return (level <= DEBUG_LEVEL);
  }

  public static void println(String s) {
    println(s, DEFAULT_LEVEL);
  }
}
