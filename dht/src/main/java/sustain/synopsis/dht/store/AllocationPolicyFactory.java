package sustain.synopsis.dht.store;

public class AllocationPolicyFactory {
    public static AllocationPolicy getAllocationPolicy(String policy) {
        switch (policy) {
            case "round-robin":
                return new RoundRobinAllocationPolicy();
            default:
                return null;
        }
    }
}
