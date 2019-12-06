package sustain.synopsis.dht.zk;

/**
 * @author Thilina Buddhika
 */

import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import sustain.synopsis.dht.ServerConstants;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Keeps a current list of cluster members and notify the listeners about membership changes.
 * Currently it only notifies about members who had joined the cluster at a later time.
 * <p>
 * Implemented as a singleton for each Node. It is expensive to maintain zk clients for every computation.
 *
 * @author Thilina Buddhika
 */
public class MembershipTracker implements AsyncCallback.ChildrenCallback {

    private static MembershipTracker instance;

    private Logger logger = Logger.getLogger(MembershipTracker.class);
    private final ZooKeeper zk;
    private ZKResourceWatcher watcher;
    private List<String> members;
    private List<MembershipListener> listeners = Collections.synchronizedList(new ArrayList<>());

    private MembershipTracker() throws ZKError {
        zk = ZooKeeperAgent.getInstance().getZooKeeperInstance();
    }

    public static MembershipTracker getInstance() throws ZKError {
        if (instance == null) {
            synchronized (MembershipTracker.class) {
                if (instance == null) {
                    instance = new MembershipTracker();
                }
            }
        }
        return instance;
    }

    public void subscribe(MembershipListener listener){
        listeners.add(listener);
    }

    public void getAvailableWorkers() {
        if (watcher == null) {
            watcher = new ZKResourceWatcher(this);
        }
        zk.getChildren(ServerConstants.ZK_NODES_ROOT, watcher, this, null);
    }

    private synchronized void processClusterChanges(List<String> currentChildren) {
        // very first invocation
        List<String> newMembers = new ArrayList<>();
        if (members == null) {
            members = new ArrayList<>();
            if (logger.isDebugEnabled()) {
                logger.debug("Started populating initial membership list...");
            }
            for (String endpoint : currentChildren) {
                addMember(endpoint);
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("New member discovered. Endpoint: %s", endpoint));
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Finished populating initial membership list...");
            }
            newMembers.addAll(members);
        } else {
            // membership has changed. One or more processes have joined the cluster
            // we should find the processors who have joined the cluster
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Membership has changed. Previous count: %d, Current count: %d",
                        members.size(), currentChildren.size()));
            }
            for (String nodeAddr : currentChildren) {
                boolean newMember = addMember(nodeAddr);
                if (newMember) {
                    newMembers.add(nodeAddr);
                }
            }
        }
        notifySubscribers(newMembers);
    }

    private void notifySubscribers(List<String> nodes){
        for(MembershipListener listener : listeners){
            listener.handleMembershipChange(nodes);
        }
    }

    @Override
    public void processResult(int rc, String path, Object o, List<String> childNodes) {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                getAvailableWorkers();
                break;
            case OK:
                processClusterChanges(childNodes);
                break;
            default:
                logger.error("Error fetching child nodes.", KeeperException.create(KeeperException.Code.get(rc), path));
        }
    }

    private boolean addMember(String endpoint) {
        if (endpoint != null && !members.contains(endpoint)) {
            members.add(endpoint);
            return true;
        }
        return false;
    }
}
