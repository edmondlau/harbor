package simpledb;
import java.util.*;

/**
 * Page is the interface used to represent pages that are resident in the
 * BufferPool.  Typically, DbFiles will read and write pages from disk.
 * <p>
 * Pages may be "dirty", indicating that they have been modified since they
 * were last written out to disk.
 *
 */
public interface Page {
  /**
   * Return the id of this page.  The id is a unique identifier for a page
   * that can be used to look up the page on disk or determine if the page
   * is resident in the buffer pool.
   * @return the id of this page
   */
  public PageId id();

  /**
   * Returns a Java iterator that iterates through the tuples of this page.
   * The type of the Object return by each call to Iterator.getNext is
   * Tuple.
   * @return The iterator.
   * @see Tuple
   */
  public Iterator iterator();

  /**
   * Returns true if this page is dirty.
   * @return dirty state of this page
   */
  public boolean isDirty();

  /**
   * Set the dirty state of this page
   */
  public void markDirty(boolean dirty);
}
