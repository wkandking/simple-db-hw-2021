package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.ArrayList;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

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
    class ListNode{
        PageId pageId;
        Page page;
        ListNode pre;
        ListNode next;
        public ListNode(){}
        public ListNode(PageId _pageId, Page _page){
            pageId = _pageId;
            page = _page;
        }
        public Page getPage() {
            return page;
        }

        public void setPage(Page page) {
            this.page = page;
        }
    }
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int numPages;

    private ConcurrentHashMap<PageId, ListNode> bufferPool;

    private LockManager lockManager;

    /** 定义双向循环链表的头和尾，方便后续操作 **/
    private ListNode head;
    private ListNode last;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        bufferPool = new ConcurrentHashMap<>();
        lockManager = new LockManager();
        head = new ListNode();
        last = new ListNode();
        head.next = last;
        last.pre = head;
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
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes
        int lockType;
        if(perm.equals(Permissions.READ_ONLY)){
            lockType = 0;
        }else{
            lockType = 1;
        }
        final long start = System.currentTimeMillis();
        long timeout = new Random().nextInt(2000) + 1000;
        while(true){
            final long cur = System.currentTimeMillis();
            if(cur - start > timeout){
                transactionComplete(tid, false);
                throw new TransactionAbortedException();
            }

            if(lockManager.acquireLock(tid, pid, lockType)){
                break;
            }
        }
        if(bufferPool.containsKey(pid)){
            ListNode node = bufferPool.get(pid);
            moveNodeToHead(node);
            return node.page;
        }else{
            Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            ListNode node = new ListNode(pid, page);
            if(bufferPool.size() == numPages){
                evictPage();
            }
            insertHeadNode(node);
            bufferPool.put(pid, node);
            return page;
        }
    }

    private void insertHeadNode(ListNode node) {
        node.next = head.next;
        head.next.pre = node;
        head.next = node;
        node.pre = head;
    }

    private void moveNodeToHead(ListNode node) {
        ListNode a = node.pre;
        ListNode b = node.next;
        if(a != head){
            a.next = b;
            b.pre = a;
            node.next = head.next;
            head.next.pre = node;
            head.next = node;
            node.pre = head;
        }
    }
    private void moveNodeToLast(ListNode node){
        ListNode a = node.pre;
        ListNode b = node.next;
        if(b != last){
            a.next = b;
            b.pre = a;
            node.pre = last.pre;
            last.pre.next = node;
            node.next = last;
            last.pre = node;
        }
    }
    private ListNode deleteLastNode() {
        ListNode node = last.pre;
        ListNode a = node.pre;
        a.next = last;
        last.pre = a;
        return node;
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
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        this.transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.hasLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if(commit){
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else{
            restorePage(tid);
        }
        for(PageId pid : bufferPool.keySet()){
            if(lockManager.hasLock(tid, pid)){
                unsafeReleasePage(tid, pid);
            }
        }
    }
    public synchronized void restorePage(TransactionId tid){
        for(PageId pid :bufferPool.keySet()){
            ListNode node = bufferPool.get(pid);
            Page page = node.getPage();
            if(page.isDirty() == tid){
                Page temp = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                node.setPage(temp);
                bufferPool.put(pid, node);
            }
        }
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
        // some code goes here
        // not necessary for lab1
        HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = file.insertTuple(tid, t);
        for(Page page : dirtyPages){
            page.markDirty(true, tid);
            ListNode node = bufferPool.get(page.getId());
            node.setPage(page);
            bufferPool.put(page.getId(), node);
            insertHeadNode(node);

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
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> dirtyPages = file.deleteTuple(tid, t);
        for(Page page : dirtyPages){
            page.markDirty(true, tid);
            ListNode node = bufferPool.get(page.getId());
            node.setPage(page);
            bufferPool.put(page.getId(), node);
            insertHeadNode(node);
        }

    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for(PageId pid : bufferPool.keySet()){
            flushPage(pid);
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        bufferPool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        ListNode listNode = bufferPool.get(pid);
        Page page = listNode.page;
        HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
        file.writePage(page);
        page.markDirty(false, null);
        listNode.setPage(page);
        bufferPool.put(pid, listNode);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pageId : bufferPool.keySet()){
            Page page = bufferPool.get(pageId).getPage();
            if(page.isDirty() == tid){
                flushPage(pageId);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for
        ListNode removeNode = last.pre;
        while(removeNode != head){
            if(removeNode.page.isDirty() != null){
                removeNode = removeNode.pre;
            }else{
                moveNodeToLast(removeNode);
                deleteLastNode();
                try {
                    flushPage(removeNode.pageId);
                    discardPage(removeNode.pageId);
                    return;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        throw new DbException("都是脏页");
    }




}
