package simpledb;

import java.io.*;
import java.util.*;
import java.util.stream.IntStream;

import static simpledb.Database.getBufferPool;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc tupleDesc;
    private final Integer id;
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        file = f;
        tupleDesc = td;
        id = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageSize = BufferPool.getPageSize();
        int offset = pageSize * pid.pageNumber();
        Page ret = null;
        byte[] data = new byte[pageSize];
        try {
            RandomAccessFile rFile = new RandomAccessFile(file, "r");
            rFile.seek(offset);
            rFile.read(data, 0, pageSize);
            ret = new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ret;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        RandomAccessFile file = new RandomAccessFile(this.file, "rw");
        file.seek((long) page.getId().pageNumber() * BufferPool.getPageSize());
        file.write(page.getPageData());
        file.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) ((file.length() + BufferPool.getPageSize() - 1) / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        for (int i = 0; i < numPages(); i++) {
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                ArrayList<Page> ret = new ArrayList<>();
                ret.add(page);
                return ret;
            }
        }
        HeapPage newPage = new HeapPage(new HeapPageId(getId(), numPages()), HeapPage.createEmptyPageData());
        newPage.insertTuple(t);
        writePage(newPage);
        ArrayList<Page> ret = new ArrayList<>();
        ret.add(newPage);
        return ret;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid,
                t.getRecordId().getPageId(), Permissions.READ_WRITE);
        p.deleteTuple(t);
        ArrayList<Page> list = new ArrayList<>();
        list.add(p);
        return list;
    }

    private class HeapFileIterator implements DbFileIterator {
        private final TransactionId tid;
        private Iterator<Tuple> iterator;
        private int idx;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
            idx = -1;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            idx = 0;
            BufferPool bp = Database.getBufferPool();
            iterator = ((HeapPage) bp.getPage(tid, new HeapPageId(getId(), idx), Permissions.READ_ONLY)).iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return iterator != null && (iterator.hasNext() || idx < numPages() - 1);
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (iterator == null) throw new NoSuchElementException();
            if (iterator.hasNext()) return iterator.next();
            if (idx >= numPages() - 1) throw new NoSuchElementException();
            idx += 1;
            BufferPool bp = Database.getBufferPool();
            iterator = ((HeapPage) bp.getPage(tid, new HeapPageId(getId(), idx), Permissions.READ_ONLY)).iterator();
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            idx = -1;
            iterator = null;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new DbFileIterator() {
            Iterator<Integer> page = Collections.emptyIterator();
            Iterator<Tuple> tuple = Collections.emptyIterator();
            @Override
            public void open() throws DbException, TransactionAbortedException {
                page = IntStream.range(0, numPages()).iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                while (!tuple.hasNext() && page.hasNext())
                    tuple = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), page.next()), Permissions.READ_ONLY)).iterator();
                return tuple.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                while (!tuple.hasNext() && page.hasNext())
                    tuple = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), page.next()), Permissions.READ_ONLY)).iterator();
                return tuple.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();
                tuple = Collections.emptyIterator();
            }

            @Override
            public void close() {
                page = Collections.emptyIterator();
                tuple = Collections.emptyIterator();
            }
        };
    }


}

