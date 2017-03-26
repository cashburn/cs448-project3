package relop;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {

  Iterator it;
  Integer[] ourFields;

  /**
   * Constructs a projection, given the underlying iterator and field numbers.
   */
  public Projection(Iterator iter, Integer... fields) {
    it = iter;
    ourFields = fields;
    this.schema = new Schema(ourFields.length);
    Schema other = it.schema;
    for(int i = 0; i < ourFields.length; i++){
      this.schema.initField(i, other.fieldType(fields[i]), other.fieldLength(fields[i]), other.fieldName(fields[i]));
    }
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
    it.restart();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    return it.isOpen();
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    it.close();
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    return isOpen() && it.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   *
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    Tuple a = it.getNext();
    Tuple b = new Tuple(this.schema);
    for(int i = 0; i < ourFields.length; i++){
      b.setField(i, a.getField(ourFields[i]));
    }
    return b;
  }

} // public class Projection extends Iterator
