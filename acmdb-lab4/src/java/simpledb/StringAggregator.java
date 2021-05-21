package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldType;
    private final Map<Field, Integer> cnt;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        cnt = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field field = null;
        if (gbfield == NO_GROUPING) {
            field = new IntField(0);
        } else {
            field = tup.getField(gbfield);
        }
        cnt.put(field, cnt.getOrDefault(field, 0) + 1);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        if (gbfield == NO_GROUPING) {
            Type[] tdType = new Type[1];
            tdType[0] = Type.INT_TYPE;
            String[] tdName = new String[1];
            tdName[0] = "AV";
            TupleDesc td = new TupleDesc(tdType, tdName);
            Tuple tuple = new Tuple(td);
            tuple.setField(0, new IntField(cnt.values().iterator().next()));
            List<Tuple> tpList = new ArrayList<>();
            tpList.add(tuple);
            return new TupleIterator(td, tpList);
        } else {
            Type[] tdType = new Type[2];
            tdType[0] = gbfieldType;
            tdType[1] = Type.INT_TYPE;
            String[] tdName = new String[2];
            tdName[0] = "GV";
            tdName[1] = "AV";
            TupleDesc td = new TupleDesc(tdType, tdName);
            return new TupleIterator(td, cnt.keySet().stream().map(k -> {
                Tuple tuple = new Tuple(td);
                tuple.setField(0, k);
                tuple.setField(1, new IntField(cnt.get(k)));
                return tuple;
            }).collect(Collectors.toCollection(ArrayList::new)));
        }
    }
}
