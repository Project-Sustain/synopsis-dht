package sustain.synopsis.dht.zk;

import java.util.List;

/**
 * @author Thilina Buddhika
 */
public interface MembershipListener {
    public void handleMembershipChange(List<String> members);
}
