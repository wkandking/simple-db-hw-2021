package simpledb.transaction;

import simpledb.storage.Page;

import java.util.HashMap;
import java.util.Map;

public class LockManager {
    class PageLockStatus{
        private boolean shareLock;
        private boolean exclusiveLock;

        public boolean isShareLock() {
            return shareLock;
        }

        public void setShareLock(boolean shareLock) {
            this.shareLock = shareLock;
        }

        public boolean isExclusiveLock() {
            return exclusiveLock;
        }

        public void setExclusiveLock(boolean exclusiveLock) {
            this.exclusiveLock = exclusiveLock;
        }
    }
    private static Map<Page, PageLockStatus> pageStatus = new HashMap<>();
    public static boolean acquireLock(Transaction t, Page page, String lockType){
        if(lockType.equals("S")){
            return true;
        }else{
            pageStatus.getOrDefault()
        }
    }

}
