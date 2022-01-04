package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;

    private int afield;
    private Op what;

    private HashMap<Field, Integer> groupValue;

    private HashMap<Field, List<Integer>> groupToAvg;


    private TupleDesc td;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here

        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        groupValue = new HashMap<>();
        groupToAvg = new HashMap<>();
        td = gbfield != NO_GROUPING ? new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE},
                new String[]{"groupValue","aggregateValue"}) :
                new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateValue"});
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupField = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        IntField aggregateFeild = (IntField) tup.getField(afield);
        int value = aggregateFeild.getValue();
        if(groupField != null && !groupField.getType().equals(gbfieldtype)){
            throw new UnsupportedOperationException();
        }
        switch (what){
            case MIN :
                groupValue.put(groupField, Math.min(groupValue.getOrDefault(groupField, value), value));
                break;
            case MAX:
                groupValue.put(groupField, Math.max(groupValue.getOrDefault(groupField, value), value));
                break;
            case SUM:
                groupValue.put(groupField, groupValue.getOrDefault(groupField, 0) + value);
                break;
            case COUNT:
                groupValue.put(groupField, groupValue.getOrDefault(groupField, 0) + 1);
                break;
            case AVG:
                List<Integer> temp = null;
                if(groupToAvg.containsKey(groupField)){
                    temp = groupToAvg.get(groupField);
                }else{
                    temp = new ArrayList();
                }
//                List<Integer> temp= groupToAvg.getOrDefault(groupField, new ArrayList<>());
                temp.add(value);
                int sum = 0;
                for(int num : temp){
                    sum += num;
                }
                groupValue.put(groupField, sum / temp.size());
                groupToAvg.put(groupField, temp);
                break;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        if(gbfield == NO_GROUPING){
            for(Field key : groupValue.keySet()){
                Tuple tuple = new Tuple(td);
                tuple.setField(0, new IntField(groupValue.get(key)));
                tuples.add(tuple);
            }
        }else{
            for(Field key : groupValue.keySet()){
                Tuple tuple = new Tuple(td);
                tuple.setField(0, key);
                tuple.setField(1, new IntField(groupValue.get(key)));
                tuples.add(tuple);
            }
        }
        return new TupleIterator(td, tuples);
    }

}
