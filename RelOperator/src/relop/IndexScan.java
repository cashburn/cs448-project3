package relop;

import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.BucketScan;

/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {

  HashIndex hashIndex;
  HeapFile heapFile;
  BucketScan bucketScan;
  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public IndexScan(Schema schema, HashIndex index, HeapFile file) {
    this.schema = schema;
    hashIndex = index;
    heapFile = file;
    bucketScan = hashIndex.openScan();
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    String s = hashIndex.toString();
    System.out.println("IndexScan: " + s);
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    //Close the current bucket scan and re-open, "as if it were just constructed"
    bucketScan.close();
    bucketScan = hashIndex.openScan();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    //make sure the iterator is open (not sure if index or bucketscan would be null, but one would have to be)
    return ((hashIndex != null) && (bucketScan != null));
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    //reset all params and close scan
    bucketScan.close();
    hashIndex = null;
    heapFile = null;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    //inherited 
    return bucketScan.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    //TODO
    if(bucketScan.hasNext()){
      return new Tuple(this.schema, heapFile.selectRecord(bucketScan.getNext()));
    } else {
      throw new IllegalStateException("No tuples");
    }
  }

  /**
   * Gets the key of the last tuple returned.
   */
  public SearchKey getLastKey() {
    //inherited
    return bucketScan.getLastKey();
  }

  /**
   * Returns the hash value for the bucket containing the next tuple, or maximum
   * number of buckets if none.
   */
  public int getNextHash() {
   //TODO
    return bucketScan.getNextHash();
  }

} // public class IndexScan extends Iterator
