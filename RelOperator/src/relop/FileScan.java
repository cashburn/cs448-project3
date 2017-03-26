package relop;

import global.RID;
import heap.HeapFile;
import heap.HeapScan;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator {

    private HeapScan hs;
    private HeapFile hf;
    private boolean open;
    private RID last;

  /**
   * Constructs a file scan, given the schema and heap file.
   */
  public FileScan(Schema schema, HeapFile file) {
      int fldcnt = schema.getCount();
      this.hf = file;
      this.schema = schema;

      /*this.schema = new Schema(fldcnt);
      for (int i = 0; i < fldcnt; i++) {
          this.schema.initField(i, schema, i);
      }*/

      hs = hf.openScan();
      open = true;
      last = new RID();

  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
      System.out.printf("FileScan: (%s)", hf);
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
      close();
      hs = hf.openScan();
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
          hs.close();
          open = false;
      }
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
      return open && hs.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   *
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
      return new Tuple(schema, hs.getNext(last));
  }

  /**
   * Gets the RID of the last tuple returned.
   */
  public RID getLastRID() {
    return new RID(last);
  }

} // public class FileScan extends Iterator
