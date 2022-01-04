package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private ConcurrentHashMap<Field, Integer> groupValue;
    private TupleDesc td;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        groupValue = new ConcurrentHashMap<>();
        td = gbfield != NO_GROUPING ? new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE},
                new String[]{"groupValue","aggregateValue"}) :
                new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateValue"});
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupField = gbfield == NO_GROUPING ? null :  tup.getField(gbfield);
        if(groupField != null && gbfieldtype.equals(groupField.getType())){
            if(what.equals(Op.COUNT)){
                groupValue.put(groupField, groupValue.getOrDefault(groupField, 0) + 1);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
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
