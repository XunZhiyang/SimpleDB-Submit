package simpledb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1, lab2 and lab3.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int n = 0;
    private final int num, ioCostPerPage;
    private final int[] min, max;
    private final Object[] hist;
    private final Map<String, Integer> map = new HashMap<>();

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        HeapFile f = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        num = f.numPages();
        this.ioCostPerPage = ioCostPerPage;
        TupleDesc td = f.getTupleDesc();
        int numFields = td.numFields();
        hist = new Object[numFields];
        min = new int[numFields];
        max = new int[numFields];
        Arrays.fill(min, Integer.MAX_VALUE);
        Arrays.fill(max, Integer.MIN_VALUE);

        Transaction transaction = new Transaction();
        DbFileIterator iterator = f.iterator(transaction.getId());
        try {
            iterator.open();
            while (iterator.hasNext()) {
                Iterator<Field> fields = iterator.next().fields();
                for (int i = 0; i < numFields; i++) {
                    Field field = fields.next();
                    if (field.getType() == Type.INT_TYPE) {
                        int value = ((IntField) field).getValue();
                        max[i] = Integer.max(max[i], value);
                        min[i] = Integer.min(min[i], value);
                    }
                }
                n++;
            }

            Iterator<TupleDesc.TDItem> tdItemIterator = td.iterator();
            for (int i = 0; i < numFields; i++) {
                TupleDesc.TDItem item = tdItemIterator.next();
                if (item.fieldName != null) map.put(item.fieldName, i);
                switch (item.fieldType) {
                    case INT_TYPE:
                        hist[i] = new IntHistogram(Integer.min(NUM_HIST_BINS, max[i] - min[i] + 1), min[i], max[i]);
                        break;
                    case STRING_TYPE:
                        hist[i] = new StringHistogram(NUM_HIST_BINS);
                        break;
                }
            }

            iterator.rewind();
            while (iterator.hasNext()) {
                Iterator<Field> fields = iterator.next().fields();
                for (int i = 0; i < numFields; i++) {
                    Field field = fields.next();
                    switch (field.getType()) {
                        case INT_TYPE:
                            ((IntHistogram) hist[i]).addValue(((IntField) field).getValue());
                            break;
                        case STRING_TYPE:
                            ((StringHistogram) hist[i]).addValue(((StringField) field).getValue());
                            break;
                    }
                }
            }
        } catch (TransactionAbortedException | DbException e) {
            e.printStackTrace();
        } finally {
            iterator.close();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return num * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) (n * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if (hist[field] instanceof IntHistogram)
            return ((IntHistogram) hist[field]).estimateSelectivity(op, ((IntField) constant).getValue());
        else if (hist[field] instanceof StringHistogram) {
            if (constant instanceof IntField)
                return ((StringHistogram) hist[field]).hist.estimateSelectivity(op, ((IntField) constant).getValue());
            else
                return ((StringHistogram) hist[field]).estimateSelectivity(op, ((StringField) constant).getValue());
        }
        return 0;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return n;
    }

    public int id(String s) {
        if (s == null) return -1;
        else return map.getOrDefault(s, -1);
    }

    public int min(int x) {
        return min[x];
    }

    public int max(int x) {
        return max[x];
    }

}
