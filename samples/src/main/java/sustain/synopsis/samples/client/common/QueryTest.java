package sustain.synopsis.samples.client.common;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import sustain.synopsis.dht.store.services.*;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Iterator;

public class QueryTest {

    public static void main(String[] args) {
        String host = args[0].split(":")[0];
        int port = Integer.parseInt(args[0].split(":")[1]);
        String datasetId = args[1];
        LocalDateTime fromDateTime = LocalDateTime.parse(args[2]);
        LocalDateTime toDateTime = LocalDateTime.parse(args[3]);

        String feature = args[4];

        ZoneOffset offset = ZoneOffset.UTC;
        if (args.length > 5) {
            offset = ZoneOffset.of(args[5]);
        }

        Channel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext().build();
        TargetedQueryServiceGrpc.TargetedQueryServiceBlockingStub stub =
                TargetedQueryServiceGrpc.newBlockingStub(channel);

        long from = fromDateTime.toEpochSecond(offset) * 1000;
        long to = toDateTime.toEpochSecond(offset) * 1000;

        Expression temporalExpression = Expression.newBuilder()
                .setExpression1(Expression.newBuilder()
                        .setPredicate1(Predicate.newBuilder()
                                .setIntegerValue(from)
                                .setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN_OR_EQUAL)
                                .build())
                        .build())
                .setCombineOp(Expression.CombineOperator.AND)
                .setExpression2(Expression.newBuilder()
                        .setPredicate1(Predicate.newBuilder()
                                .setIntegerValue(to)
                                .setComparisonOp(Predicate.ComparisonOperator.LESS_THAN)
                                .build())
                        .build())
                .build();

        TargetQueryRequest req = TargetQueryRequest.newBuilder()
                .setTemporalScope(temporalExpression)
                .setDataset(datasetId)
                .addFeaturePredicates(Expression.newBuilder()
                        .build())
                .build();

        Iterator<TargetQueryResponse> responses = stub.query(req);

        while (responses.hasNext()) {
            TargetQueryResponse resp = responses.next();
            System.out.println(resp);
        }
    }

}
