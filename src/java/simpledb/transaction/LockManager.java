package simpledb.transaction;

import simpledb.storage.Page;
import simpledb.storage.PageId;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

public class LockManager {
    class LockStatus{
        private TransactionId tid;
        private int lockType; // 1 为 X  0 为 S

        public LockStatus(TransactionId tid, int lockType) {
            this.tid = tid;
            this.lockType = lockType;
        }

        public TransactionId getTid() {
            return tid;
        }

        public void setTid(TransactionId tid) {
            this.tid = tid;
        }

        public int getLockType() {
            return lockType;
        }

        public void setLockType(int lockType) {
            this.lockType = lockType;
        }
    }
    private volatile Map<PageId, Vector<LockStatus>> pagesLocks;

    public LockManager() {
        pagesLocks = new HashMap<>();
    }
    public synchronized boolean acquireLock(TransactionId tid, PageId pid, int lockType){
        if(!pagesLocks.containsKey(pid) || pagesLocks.get(pid).size() == 0){ // 无锁上锁
            LockStatus lock = new LockStatus(tid, lockType);
            Vector<LockStatus> pageLocks = new Vector<>();
            pageLocks.add(lock);
            pagesLocks.put(pid, pageLocks);
            return true;
        }else{ // 已经存在锁
            int pageStatus = 0;
            Vector<LockStatus> pageLockStatuses = pagesLocks.get(pid);
            for(LockStatus lock : pageLockStatuses){
                if(lock.lockType == 1){
                    pageStatus = 1;
                }
                if(lock.tid.equals(tid)){ // 找到当前事务的锁
                    if(lock.lockType == lockType){ //事务之前加的锁 和现在加的锁一样，可以加锁，类似于可重入锁
                        return true;
                    }else{
                        if(lock.lockType == 1){ // 排他锁 > 共享锁 ， 所以若已经有了排他锁，则不需要再加共享锁
                            return true;
                        }else{
                            if(pageLockStatuses.size() == 1){ //有了共享锁，但是只有一个事务在page上面加锁，则锁升级
                                lock.lockType = 1;
                                return true;
                            }else{
                                return false;
                            }
                        }
                    }
                }
            }
            // 如果没有当前事务的锁
            if(pageStatus == 1){// 说明有事务在Page上加了X锁
                return false;
            }else{
                if(lockType == 0){
                    LockStatus lock = new LockStatus(tid, lockType);
                    pageLockStatuses.add(lock);
                    pagesLocks.put(pid, pageLockStatuses);
                    return true;
                }else{
                    return false;
                }
            }
        }
    }
    public synchronized boolean releaseLock(TransactionId tid, PageId pid){

        Vector<LockStatus> lockStatuses = pagesLocks.get(pid);
        for(LockStatus lock : lockStatuses){
            if(lock.tid.equals(tid)){
                lockStatuses.remove(lock);
                if(lockStatuses.size() == 0){
                    pagesLocks.remove(pid);
                }
                return true;
            }
        }
        return false;
    }
    public synchronized boolean hasLock(TransactionId tid, PageId pid){
        if(!pagesLocks.containsKey(pid) || pagesLocks.get(pid) == null){
            return false;
        }
        final Vector<LockStatus> lockStatuses = pagesLocks.get(pid);
        for(LockStatus lock : lockStatuses){
            if(lock.tid.equals(tid)){
                return true;
            }
        }
        return false;
    }
}
