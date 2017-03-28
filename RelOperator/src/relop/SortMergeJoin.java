package relop;

import global.SearchKey;
import global.RID;
import global.GlobalConst;
import global.AttrType;
import heap.HeapFile;
import heap.HeapScan;
import java.util.Collections;
import java.util.Comparator;
import java.util.ArrayList;

public class SortMergeJoin extends Iterator {

    Iterator left;
    Iterator right;
    HeapFile leftFile, rightFile, result;
    HeapScan leftScan, rightScan;
    int lcol, rcol;
    int numLTuples, numRTuples;

    Tuple nextTuple;
    boolean nextConsumed;
    boolean leftHas, rightHas;

    boolean open;

    public SortMergeJoin(Iterator left, Iterator right, Integer lcol, Integer rcol) {
        this.left = left;
        this.right = right;
        this.lcol = lcol;
        this.rcol = rcol;
        schema = Schema.join(left.schema, right.schema);
        numLTuples = GlobalConst.PAGE_SIZE / left.schema.getLength();
        numRTuples = GlobalConst.PAGE_SIZE / right.schema.getLength();
        leftFile = sort(left, lcol, numLTuples);
        rightFile = sort(right, rcol, numRTuples);
        result = new HeapFile(null);
        leftScan = leftFile.openScan();
        rightScan = rightFile.openScan();


        open = true;
        /*
        System.out.printf("Page Size: %d\n", GlobalConst.PAGE_SIZE);
        System.out.printf("Left Tuple Size: %d\n", left.schema.getLength());
        System.out.printf("Right Tuple Size: %d\n", right.schema.getLength());
        System.out.printf("Left Tuples/Page: %d\n", GlobalConst.PAGE_SIZE/left.schema.getLength());
        System.out.printf("Right Tuples/Page: %d\n", GlobalConst.PAGE_SIZE/right.schema.getLength());
        */
    }

    private HeapFile sort(Iterator iter, final int col, int bufferSize) {
        ArrayList<Tuple> sortList = new ArrayList<Tuple>(bufferSize);
        ArrayList<RID> rids = new ArrayList<RID>();
        ArrayList<RID> rids2 = new ArrayList<RID>();
        HeapFile leftTemp = new HeapFile(null);
        HeapFile rightTemp = new HeapFile(null);
        HeapFile resultTemp = new HeapFile(null);
        HeapScan leftScan = leftTemp.openScan();
        HeapScan rightScan = rightTemp.openScan();
        HeapScan resultScan = resultTemp.openScan();
        boolean hasNext = iter.hasNext();
        final int fieldType = iter.schema.fieldType(col);
        while (hasNext) {
            for (int i = 0; hasNext && i < bufferSize; hasNext = iter.hasNext(), i++) {
                sortList.add(iter.getNext());
                //leftTemp.insertRecord(iter.getNext().getData());
            }
            //while (leftScan.hasNext()) {
                //sortList.add(leftScan.getNext());
            //}
            if (fieldType == AttrType.INTEGER) {
                Collections.sort(sortList, new Comparator<Tuple>(){
    				public int compare(Tuple t1, Tuple t2)
    				{
    					return t1.getIntFld(col) - t2.getIntFld(col);
    				}
			    });
            }
            else if (fieldType == AttrType.STRING) {
                Collections.sort(sortList, new Comparator<Tuple>(){
    				public int compare(Tuple t1, Tuple t2)
    				{
    					return t1.getStringFld(col).compareTo(t2.getStringFld(col));
    				}
			    });
            }
            else if (fieldType == AttrType.FLOAT) {
                Collections.sort(sortList, new Comparator<Tuple>(){
    				public int compare(Tuple t1, Tuple t2)
    				{
    					return new Float(t1.getFloatFld(col)).compareTo(t2.getFloatFld(col));
    				}
			    });
            }
            for (int i = 0; i < sortList.size(); i++) {
                RID tmp = resultTemp.insertRecord(sortList.get(i).getData());
                if (i == 0)
                    rids.add(tmp);
            }
            sortList.clear();
        }

        // resultScan.close();
        // resultScan = resultTemp.openScan();
        while (rids.size() > 1) {
            for (int i = 1; i + 1 < rids.size(); i += 2) {
                while (resultScan.hasNext()) {
                    RID tmp = new RID();
                    byte[] tuple = resultScan.getNext(tmp);
                    if (tmp == rids.get(i))
                        break;
                    leftTemp.insertRecord(tuple);
                    resultTemp.deleteRecord(tmp);
                }
                while (resultScan.hasNext()) {
                    RID tmp = new RID();
                    byte[] tuple = resultScan.getNext(tmp);
                    if (tmp == rids.get(i+1))
                        break;
                    rightTemp.insertRecord(tuple);
                    resultTemp.deleteRecord(tmp);
                }

                leftScan.close();
                rightScan.close();
                leftScan = leftTemp.openScan();
                rightScan = rightTemp.openScan();
                RID ridR = new RID();
                RID ridL = new RID();
                Tuple tupleL = null;
                Tuple tupleR = null;
                boolean leftHas, rightHas;
                if (leftHas = leftScan.hasNext())
                    tupleL = new Tuple(iter.schema, leftScan.getNext(ridL));
                if (rightHas = rightScan.hasNext())
                    tupleR = new Tuple(iter.schema, rightScan.getNext(ridR));
                int j = 0;
                while ((leftHas = leftScan.hasNext()) && (rightHas = rightScan.hasNext())) {

                    if (fieldType == AttrType.INTEGER) {
                        if (tupleL.getIntFld(col) < tupleR.getIntFld(col)) {
                            RID tmp = resultTemp.insertRecord(tupleL.getData());
                            tupleL = new Tuple(iter.schema, leftScan.getNext(ridL));
                            if (j == 0) {
                                rids2.add(tmp);
                                j++;
                            }
                        }
                        else {
                            RID tmp = resultTemp.insertRecord(tupleR.getData());
                            tupleR = new Tuple(iter.schema, rightScan.getNext(ridR));
                            if (j == 0) {
                                rids2.add(tmp);
                                j++;
                            }
                        }
                    }
                    else
                        throw new UnsupportedOperationException("String cmp not implemented");
                }
                if (leftHas)
                    resultTemp.insertRecord(tupleL.getData());
                while (leftScan.hasNext()) {
                    resultTemp.insertRecord(leftScan.getNext(ridL));
                }
                if (rightHas)
                    resultTemp.insertRecord(tupleR.getData());
                while (leftScan.hasNext()) {
                    resultTemp.insertRecord(rightScan.getNext(ridR));
                }
            }
            rids = rids2;
            rids2 = new ArrayList<RID>();
            resultScan.close();
            resultScan = resultTemp.openScan();
        }

        iter.restart();
        resultScan.close();
        resultScan = resultTemp.openScan();
        // System.out.println("New HeapFile:");
        // while (resultScan.hasNext())
        //     new Tuple(iter.schema, resultScan.getNext(new RID())).print();
        // System.out.println("New Original:");
        // while (iter.hasNext())
        //     iter.getNext().print();
        return resultTemp;
    }

    public void close() {
        leftScan.close();
        rightScan.close();
        left.close();
        right.close();
        //leftFile.deleteFile();
        //rightFile.deleteFile();

        open = false;
    }

    public void explain(int depth) {
        //TODO: Implement explain
    }

    public boolean isOpen() {
        return open;
    }

    public void restart() {
        close();
        leftFile = sort(left, lcol, numLTuples);
        rightFile = sort(right, rcol, numRTuples);
        result = new HeapFile(null);
        leftScan = leftFile.openScan();
        rightScan = rightFile.openScan();

        open = true;
    }

    public boolean hasNext() {
        RID rid = new RID();
        RID ridR = new RID();
        RID ridL = new RID();
        Tuple tupleL = null;
        Tuple tupleR = null;
        leftHas = leftScan.hasNext();
        rightHas = rightScan.hasNext();
        if (leftHas && rightHas) {
            tupleL = new Tuple(left.schema, leftScan.getNext(ridL));
            tupleR = new Tuple(right.schema, rightScan.getNext(ridR));
        }
        else {
            return false;
        }
        while (true) {
            if (left.schema.fieldType(lcol) == AttrType.INTEGER && right.schema.fieldType(rcol) == AttrType.INTEGER) {
                if (tupleL.getIntFld(lcol) == tupleR.getIntFld(rcol)) {
                    nextTuple = Tuple.join(tupleL, tupleR, this.schema);
                    nextConsumed = false;
                    return true;
                }

                else if (tupleL.getIntFld(lcol) < tupleR.getIntFld(rcol)) {
                    if (leftHas = leftScan.hasNext())
                        tupleL = new Tuple(left.schema, leftScan.getNext(ridL));
                    else
                        return false;
                }
                else {
                    if (rightHas = rightScan.hasNext())
                        tupleR = new Tuple(right.schema, rightScan.getNext(ridR));
                    else
                        return false;
                }
            }
            else
                throw new UnsupportedOperationException("String cmp not implemented");
        }
    }

    public Tuple getNext() {
        if (nextConsumed)
            throw new IllegalStateException("No tuples");
        nextConsumed = true;
        return nextTuple;
    }

}
