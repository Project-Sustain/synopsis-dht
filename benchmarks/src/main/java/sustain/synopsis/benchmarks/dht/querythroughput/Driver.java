package sustain.synopsis.benchmarks.dht.querythroughput;

import sustain.synopsis.benchmarks.dht.BenchmarkBaseNode;
import sustain.synopsis.dht.services.query.DHTQueryProcessor;
import sustain.synopsis.dht.store.StorageException;
import sustain.synopsis.dht.store.node.NodeStore;
import sustain.synopsis.dht.store.services.Expression;
import sustain.synopsis.dht.store.services.Predicate;
import sustain.synopsis.dht.store.services.TargetQueryRequest;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

public class Driver extends BenchmarkBaseNode {

    private final int readerThreadPoolCount;
    private final int tasksPerReq;
    private DHTQueryProcessor dhtQueryProcessor;

    public Driver(String configFile, int readerThreadPoolCount, int tasksPerReq) {
        super(configFile);
        this.readerThreadPoolCount = readerThreadPoolCount;
        this.tasksPerReq = tasksPerReq;
    }

    private void init() throws StorageException, InterruptedException {
        initContext();
        NodeStore nodeStore = new NodeStore();
        nodeStore.init();
        this.dhtQueryProcessor =
                new DHTQueryProcessor(nodeStore, Executors.newFixedThreadPool(readerThreadPoolCount), tasksPerReq);
        start();
    }

    private void launchQueries() {
        DummyQueryResponseHandler responseHandler = new DummyQueryResponseHandler();
        CompletableFuture<Boolean> future = dhtQueryProcessor.process(buildQueryRequest(), responseHandler);
        future.thenAccept(r -> {
            System.out.println(
                    "Response size (GB): " + responseHandler.getResponseSize() / (1024D * 1024 * 1024) + ", Strand "
                    + "count: " + responseHandler.getStrandCount());
        });
    }

    public static void main(String[] args) throws InterruptedException, StorageException {
        if (args.length < 3) {
            System.err.println("Usage: Driver config_file worker_count tasks_per_req");
        }
        String config = args[0];
        int workerCount = Integer.parseInt(args[1]);
        int tasksPerReq = Integer.parseInt(args[2]);
        System.out.println(String.format("Using config: %s, Worker Count: %d, Tasks Per Query: %d", config, workerCount,
                                         tasksPerReq));
        Driver driver = new Driver(config, workerCount, tasksPerReq);
        driver.init();
        driver.launchQueries();
    }

    private TargetQueryRequest buildQueryRequest() {
        Predicate fromPredicate =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN_OR_EQUAL)
                         .setIntegerValue(0).build();
        Predicate toPredicate =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.LESS_THAN).setIntegerValue(1167706)
                         .build();
        Expression temporalExp =
                Expression.newBuilder().setPredicate1(fromPredicate).setCombineOp(Expression.CombineOperator.AND)
                          .setPredicate2(toPredicate).build();

        return TargetQueryRequest.newBuilder().setDataset("noaa")
                                 .addSpatialScope(Predicate.newBuilder().setStringValue("").build())
                                 .setTemporalScope(temporalExp).build();
    }
}
