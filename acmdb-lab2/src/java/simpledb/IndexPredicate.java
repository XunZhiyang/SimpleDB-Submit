package simpledb;

import java.io.Serializable;

/**
 * IndexPredicate compares a field which has index on it against a given value
 * @see simpledb.IndexDbIterator
 */
public class IndexPredicate implements Serializable {
	
    private static final long serialVersionUID = 1L;

    private final Predicate.Op operator;
    private final Field fvalue;

    /**
     * Constructor.
     *
     * @param fvalue The value that the predicate compares against.
     * @param op The operation to apply (as defined in Predicate.Op); either
     *   Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN, Predicate.Op.EQUAL,
     *   Predicate.Op.GREATER_THAN_OR_EQ, or Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public IndexPredicate(Predicate.Op op, Field fvalue) {
        this.operator = op;
        this.fvalue = fvalue;
    }

    public Field getField() {
        return fvalue;
    }

    public Predicate.Op getOp() {
        return operator;
    }

    /** Return true if the fieldvalue in the supplied predicate
        is satisfied by this predicate's fieldvalue and
        operator.
        @param ipd The field to compare against.
    */
    public boolean equals(IndexPredicate ipd) {
        return fvalue.compare(operator, ipd.fvalue);
    }

}
