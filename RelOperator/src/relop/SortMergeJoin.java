package relop;

public class SortMergeJoin extends Iterator {
    public SortMergeJoin(Iterator left, Iterator right, Integer lcol, Integer rcol) {
            
    }
    
    public void close() {
        //TODO: Implement close    
    }

    public void explain(int depth) {
        //TODO: Implement explain
    }

    public Tuple getNext() {
        return new Tuple(new Schema(1));    
    }
    
    public boolean hasNext() {
        return true;
    }

    public boolean isOpen() {
        return true;    
    }

    public void restart() {
        //TODO: Restart    
    }
	
}
