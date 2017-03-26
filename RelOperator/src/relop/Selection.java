package relop;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends Iterator {

    private Iterator iter;
    private Predicate[] preds;
    private boolean startSelect;
    private Tuple nextTuple;


  /**
   * Constructs a selection, given the underlying iterator and predicates.
   */
  public Selection(Iterator iter, Predicate... preds) {
      this.preds = preds;
      this.iter = iter;
      this.schema = iter.schema;
      startSelect = true;
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
      iter.restart();
      startSelect = true;
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
      return iter.isOpen();
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    iter.close();
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
      if (!iter.hasNext())
          return false;

      Tuple tuple;
      boolean hasNext = false;
      while (iter.hasNext()) {
          tuple = iter.getNext();
          for (int i = 0; i < preds.length; i++) {
              if (preds[i].evaluate(tuple)) {
                  nextTuple = tuple;
                  return true;
              }
          }
      }
      return false;
  }

  /**
   * Gets the next tuple in the iteration.
   *
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
      //if (!hasNext())
        //throw new IllegalStateException("No more tuples");
      Tuple temp = nextTuple;
      nextTuple = null;
      return temp;
  }

} // public class Selection extends Iterator
