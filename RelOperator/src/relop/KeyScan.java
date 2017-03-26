package relop;

import global.SearchKey;
import global.RID;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;

/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {

    private HashScan scan;
    private HashIndex index;
    private HeapFile file;
    private SearchKey key;
    private boolean open;

  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public KeyScan(Schema schema, HashIndex index, SearchKey key, HeapFile file) {
      this.schema = schema;
      this.index = index;
      this.key = key;
      this.file = file;
      scan = index.openScan(key);
      open = true;
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
      System.out.printf("KeyScan: (%s)", index);
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
      close();
      scan = index.openScan(key);
      open = true;
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
      return open;
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
      if (open) {
          scan.close();
          open = false;
      }
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
      return open && scan.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   *
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
      /*if (!hasNext())
        throw new IllegalStateException("No remaining tuples");
      */
      RID rid = scan.getNext();
      return new Tuple(schema, file.selectRecord(rid));
  }

} // public class KeyScan extends Iterator
