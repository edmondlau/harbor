package simpledb;

import java.util.*;

/**
 * The common interface for any class that can compute an aggregate over a
 * list of Fields.
 */
public interface Aggregator {
  final public static int MIN = 0;
  final public static int MAX = 1;
  final public static int SUM = 2;
  final public static int AVG = 3;
  final public static int COUNT = 4;

  /**
   * Computes and returns an aggregate over a list of Fields.
   */
  public Field execute(List list);
}
