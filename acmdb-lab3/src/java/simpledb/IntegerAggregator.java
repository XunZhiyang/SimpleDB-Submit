package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final Type gbfieldType;
    private final int gbfield, afield;
    private final Op what;
    private final Map<Field, Integer> cnt, val;

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
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.what = what;
        cnt = new HashMap<>();
        val = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field gf = null;
        if (gbfield == NO_GROUPING) {
            gf = new IntField(0);
        } else {
            gf = tup.getField(gbfield);
        }
        cnt.put(gf, cnt.getOrDefault(gf, 0) + 1);
        int f = ((IntField) tup.getField(afield)).getValue();
        switch (what) {
            case MIN:
                val.put(gf, Integer.min(val.getOrDefault(gf, Integer.MAX_VALUE), f));
                break;
            case MAX:
                val.put(gf, Integer.max(val.getOrDefault(gf, Integer.MIN_VALUE), f));
                break;
            case SUM:
            case AVG:
            case SUM_COUNT:
                val.put(gf, val.getOrDefault(gf, 0) + f);
                break;
            case COUNT:
                break;
            case SC_AVG:
                assert false;
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        if (gbfield == NO_GROUPING) {
            Type[] tdType = new Type[1];
            tdType[0] = Type.INT_TYPE;
            String[] tdName = new String[1];
            tdName[0] = "AV";
            TupleDesc td = new TupleDesc(tdType, tdName);
            Tuple tuple = new Tuple(td);
            switch (what) {
                case MIN:
                case MAX:
                case SUM:
                    tuple.setField(0, new IntField(val.values().iterator().next()));
                    break;
                case AVG:
                    tuple.setField(0, new IntField(val.values().iterator().next() / cnt.values().iterator().next()));
                    break;
                case COUNT:
                    tuple.setField(0, new IntField(cnt.values().iterator().next()));
                    break;
                case SUM_COUNT:
                case SC_AVG:
                    assert false;
            }
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
                switch (what) {
                    case MIN:
                    case MAX:
                    case SUM:
                        tuple.setField(1, new IntField(val.get(k)));
                        break;
                    case AVG:
                        tuple.setField(1, new IntField(val.get(k) / cnt.get(k)));
                        break;
                    case COUNT:
                        tuple.setField(1, new IntField(cnt.get(k)));
                        break;
                    case SUM_COUNT:
                    case SC_AVG:
                        assert false;
                }
                return tuple;
            }).collect(Collectors.toCollection(ArrayList::new)));
        }
    }

}
