package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private final TransactionId t;
    private final DbIterator child;
    private boolean first = true;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.t = t;
        this.child = child;
    }

    public TupleDesc getTupleDesc() {
        Type[] types = new Type[1];
        types[0] = Type.INT_TYPE;
        String[] names = new String[1];
        return new TupleDesc(types, names);
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
    }

    public void close() {
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (!first) return null;
        first = false;
        int cnt = 0;
        child.open();
        while (child.hasNext()) {
            Tuple cur = child.next();
            try {
                Database.getBufferPool().deleteTuple(t, cur);
                cnt++;
            } catch (IOException e) {
                throw new DbException("");
            }
        }
        child.close();
        Tuple ret = new Tuple(getTupleDesc());
        ret.setField(0, new IntField(cnt));
        return ret;
    }

    @Override
    public DbIterator[] getChildren() {
        DbIterator[] children = new DbIterator[1];
        setChildren(children);
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        children[0] = child;
    }


}
