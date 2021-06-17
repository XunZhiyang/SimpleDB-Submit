package simpledb;

import java.util.TreeMap;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */


    private static class Fenwick {
        int siz;
        int[] data;

        Fenwick(int siz) {
            this.siz = siz;
            data = new int[siz + 1];
        }

        void modify(int i) {
            for (i++; i <= siz; i += i & -i) data[i]++;
        }

        private int query(int i) {
            int ret = 0;
            for (; i > 0; i -= i & -i) ret += data[i];
            return ret;
        }

        int q(int l, int r) {
            return query(r + 1) - query(l);
        }
    }

    private final int buckets;
    private final int[] s, p;
    private final TreeMap<Integer, Integer> map = new TreeMap<>();
    private final Fenwick fenwick;

    private static int div(long lhs, int rhs) {
        return (int)(lhs >= 0 ? (lhs / rhs) : (lhs - rhs + 1) / rhs);
    }

    public IntHistogram(int buckets, int min, int max) {
        this.buckets = buckets;
        p = new int[buckets];
        s = new int[buckets];
        fenwick = new Fenwick(buckets);
        max++;
        for (int i = 0; i < buckets; i++) {
            int c = div((long) min * (buckets - i) + (long) max * i, buckets);
            int np = div((long) min * (buckets - i - 1) + (long) max * (i + 1), buckets);
            map.put(c, i);
            p[i] = c;
            s[i] = np - c;
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        fenwick.modify(map.floorEntry(v).getValue());
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */



    public double estimateSelectivity(Predicate.Op op, int v) {
        if (v < p[0])
            return op == Predicate.Op.GREATER_THAN || op == Predicate.Op.GREATER_THAN_OR_EQ || op == Predicate.Op.NOT_EQUALS ? 1 : 0;
        if (v >= p[buckets - 1] + s[buckets - 1])
            return op == Predicate.Op.LESS_THAN || op == Predicate.Op.LESS_THAN_OR_EQ || op == Predicate.Op.NOT_EQUALS ? 1 : 0;
        int i = map.floorEntry(v).getValue();
        double val = 0;
        switch (op) {
            case EQUALS:
                val = (double) fenwick.q(i, i) / s[i];
                break;
            case GREATER_THAN:
                val = fenwick.q(i + 1, buckets - 1) + (double) fenwick.q(i, i) * (s[i] - v + p[i] - 1) / s[i];
                break;
            case LESS_THAN:
                val = fenwick.q(0, i - 1) + (double) fenwick.q(i, i) * (v - p[i]) / s[i];
                break;
            case LESS_THAN_OR_EQ:
                val = fenwick.q(0, i - 1) + (double) fenwick.q(i, i) * (v - p[i] + 1) / s[i];
                break;
            case GREATER_THAN_OR_EQ:
                val = fenwick.q(i + 1, buckets - 1) + (double) fenwick.q(i, i) * (s[i] - v + p[i]) / s[i];
                break;
            case NOT_EQUALS:
                val = fenwick.q(0, buckets - 1) - (double) fenwick.q(i, i) / s[i];
        }
        return val / fenwick.q(0, buckets - 1);
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        return p[0] + " # " + (p[buckets - 1] + s[buckets - 1]);

    }
}
