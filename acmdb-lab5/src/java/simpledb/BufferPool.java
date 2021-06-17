package simpledb;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {

    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private static class Graph{
        private static class WLock {
            public Lock lock;
            public boolean w;

            public WLock(Lock lock, boolean w) {
                this.lock = lock;
                this.w = w;
            }
        }

        private static class TransW {
            public TransactionId tid;
            public boolean w;

            public TransW(TransactionId tid, boolean w) {
                this.tid = tid;
                this.w = w;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                TransW rhs = (TransW) o;

                if (w != rhs.w) return false;
                return tid.equals(rhs.tid);
            }

            @Override
            public int hashCode() {
                int result = tid.hashCode();
                result = 31 * result + (w ? 1 : 0);
                return result;
            }
        }

        private final Map<TransactionId, WLock> locks = new ConcurrentHashMap<>();
        private final Map<Lock, Set<TransW>> tids = new ConcurrentHashMap<>();

        private boolean check(TransactionId start, TransactionId cur) {
            WLock s = locks.get(cur);
            if (s == null) return false;
            for (TransW t : tids.getOrDefault(s.lock, new HashSet<>())) {
                if (!s.w && !t.w) continue;
                if (t.tid == cur) continue;
                if (t.tid == start || check(start, t.tid)) return true;
            }
            return false;
        }

        public synchronized boolean wait(TransactionId tid, Lock sema, boolean w) {
            locks.put(tid, new WLock(sema, w));
            if (check(tid, tid)) {
                locks.remove(tid);
                return true;
            } else return false;
        }

        public synchronized void acquire(TransactionId tid, Lock sema, boolean w) {
            locks.remove(tid);
            tids.computeIfAbsent(sema, s -> new HashSet<>()).add(new TransW(tid, w));
        }

        public synchronized void release(TransactionId tid, Lock sema, boolean w) {
            Set<TransW> set = tids.get(sema);
            set.remove(new TransW(tid, w));
            if (set.isEmpty()) tids.remove(sema);
        }
    }

    private class Lock {
        private final Semaphore r = new Semaphore(1);
        private final Semaphore w = new Semaphore(1);
        private int cnt = 0;

        public void lock1(TransactionId tid) throws TransactionAbortedException {
            if (graph.wait(tid, this, false)) throw new TransactionAbortedException();
            try {
                r.acquire();
                if (++cnt == 1) w.acquire();
                r.release();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            graph.acquire(tid, this, false);
        }

        public void unlock1(TransactionId tid) {
            try {
                r.acquire();
                if (--cnt == 0) w.release();
                r.release();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            graph.release(tid, this, false);
        }

        public void lock2(TransactionId tid) throws TransactionAbortedException {
            if (graph.wait(tid, this, true)) throw new TransactionAbortedException();
            try {
                w.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            graph.acquire(tid, this, true);
        }

        public void unlock2(TransactionId tid) {
            w.release();
            graph.release(tid, this, true);
        }

        public void upgrade(TransactionId tid) throws TransactionAbortedException {
            if (graph.wait(tid, this, true)) throw new TransactionAbortedException();
            try {
                r.acquire();
                if (--cnt == 0) w.release();
                r.release();
                w.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            graph.release(tid, this, false);
            graph.acquire(tid, this, true);
        }
    }
    private static class TL {
        private final TransactionId tid;
        private final Lock lock;
        private Boolean w;

        public TL(TransactionId tid, Lock lock) {
            this.tid = tid;
            this.lock = lock;
            this.w = null;
        }

        public boolean isWrite() {
            return w != null && w;
        }

        public void unlock() {
            if (w != null) {
                if (w) lock.unlock2(tid);
                else lock.unlock1(tid);
                w = null;
            }
        }

        public void update(boolean w) throws TransactionAbortedException {
            if (this.w == null && !w) {
                lock.lock1(tid);
                this.w = false;
            } else if (this.w == null) {
                lock.lock2(tid);
                this.w = true;
            } else if (!this.w && w) {
                lock.upgrade(tid);
                this.w = true;
            }
        }
    }
    private static class TP {
        private final TransactionId tid;
        private final PageId pid;

        public TransactionId getTid() {
            return tid;
        }

        public PageId getPid() {
            return pid;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TP that = (TP) o;

            if (!tid.equals(that.tid)) return false;
            return pid.equals(that.pid);
        }

        @Override
        public int hashCode() {
            int result = tid.hashCode();
            result = 31 * result + pid.hashCode();
            return result;
        }

        public TP(TransactionId tid, PageId pid) {
            this.tid = tid;
            this.pid = pid;
        }
    }

    private final int pageNumber;

    private final Map<PageId, Page> pageMap;

    private final Map<PageId, Lock> lockMap;
    private final Map<TP, TL> tlMap;
    private final Graph graph;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        pageNumber = numPages;
        pageMap = new ConcurrentHashMap<>();
        lockMap = new ConcurrentHashMap<>();
        tlMap = new ConcurrentHashMap<>();
        graph = new Graph();
    }

    public static int getPageSize() {
      return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        Page ret = pageMap.get(pid);
        if (ret == null) {
            if (pageMap.size() == pageNumber) evictPage();
            ret = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            pageMap.put(pid, ret);
        }
        Lock lock = lockMap.computeIfAbsent(pid, p -> new Lock());
        TL info = tlMap.computeIfAbsent(new TP(tid, pid), p -> new TL(tid, lock));
        info.update(perm == Permissions.READ_WRITE);
        return ret;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        TL tl = tlMap.get(new TP(tid, pid));
        if (tl != null) {
            tl.unlock();
            tlMap.remove(new TP(tid, pid));
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return tlMap.containsKey(new TP(tid, p));
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        if (commit) flushPages(tid);
        else {
            for (Map.Entry<TP, TL> entry : tlMap.entrySet())
                if (entry.getKey().getTid().equals(tid) && entry.getValue().isWrite())
                    discardPage(entry.getKey().getPid());
        }
        for (Map.Entry<TP, TL> entry : tlMap.entrySet())
            if (entry.getKey().getTid().equals(tid))
                entry.getValue().unlock();
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile dbfile = Database.getCatalog().getDatabaseFile(tableId);
        for (Page page : dbfile.insertTuple(tid, t)) {
            if (!pageMap.containsKey(page.getId()) && pageMap.size() == pageNumber) evictPage();
            pageMap.put(page.getId(), page);
            page.markDirty(true, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile dbfile = Database.getCatalog().getDatabaseFile(tableId);
        for (Page page : dbfile.deleteTuple(tid, t)) {
            if (!pageMap.containsKey(page.getId()) && pageMap.size() == pageNumber) evictPage();
            pageMap.put(page.getId(), page);
            page.markDirty(true, tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public void flushAllPages() throws IOException {
        for (PageId pageId : new ArrayList<>(pageMap.keySet())) {
            flushPage(pageId);
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.

        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public void discardPage(PageId pid) {
        pageMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private void flushPage(PageId pid) throws IOException {
        if (pageMap.containsKey(pid) && pageMap.get(pid).isDirty() != null) {
            Page page = pageMap.get(pid);
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public void flushPages(TransactionId tid) throws IOException {
        for (Map.Entry<TP, TL> entry : tlMap.entrySet())
            if (entry.getKey().getTid().equals(tid) && entry.getValue().isWrite())
                flushPage(entry.getKey().getPid());
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        Iterator<Map.Entry<PageId, Page>> iterator = pageMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<PageId, Page> entry = iterator.next();
            if (entry.getValue().isDirty() != null) continue;
            try {
                flushPage(entry.getKey());
                iterator.remove();
                return;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        throw new DbException("All dirty");
    }

}
