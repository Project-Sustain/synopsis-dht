package sustain.synopsis.samples.client.query;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import sustain.synopsis.common.CommonUtil;
import sustain.synopsis.common.ProtoBuffSerializedStrand;
import sustain.synopsis.dht.store.services.*;

import java.time.LocalDateTime;
import java.time.Month;
import java.util.Iterator;

public class NOAAQueryClient {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: NOAAQueryClient proxy_url");
            return;
        }
        // initialize the stub
        System.out.println("Using proxy url: " + args[0]);
        String host = args[0].split(":")[0];
        int port = Integer.parseInt(args[0].split(":")[1]);
        Channel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        TargetedQueryServiceGrpc.TargetedQueryServiceBlockingStub stub =
                TargetedQueryServiceGrpc.newBlockingStub(channel);

        // Temporal predicate - should be provided using epochs (UTC)
        // t >= 00:00 Jan 01, 2015
        Predicate fromPredicate =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN_OR_EQUAL)
                         .setIntegerValue(
                                 CommonUtil.localDateTimeToEpoch(LocalDateTime.of(2015, Month.JANUARY, 1, 0, 0)))
                         .build();
        // t < 23:59 Jan 31, 2015
        Predicate toPredicate = Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.LESS_THAN)
                                         .setIntegerValue(CommonUtil.localDateTimeToEpoch(
                                                 LocalDateTime.of(2015, Month.JANUARY, 31, 23, 59))).build();
        // combine both predicates such that 00:00 Jan 01, 2015 =< t < 23:59 Jan 31, 2015
        Expression temporalExp =
                Expression.newBuilder().setPredicate1(fromPredicate).setCombineOp(Expression.CombineOperator.AND)
                          .setPredicate2(toPredicate).build();

        String[] geohashPrefixes = new String[]{"9y8b9", "9y8b", "9y8", "9y", "9"};

        // run queries for each of the geohashes
        for (String geohash : geohashPrefixes) {
            Predicate spatialPredicate = Predicate.newBuilder().setStringValue(geohash).build();
            // I've set the dataset id to be noaa_2015_jan during ingestion.
            TargetQueryRequest targetQueryRequest =
                    TargetQueryRequest.newBuilder().setDataset("noaa_2015_jan").addSpatialScope(spatialPredicate)
                                      .setTemporalScope(temporalExp).build();

            long fetchedData = 0;
            long t1 = System.currentTimeMillis();
            Iterator<TargetQueryResponse> queryResponseIterator = stub.query(targetQueryRequest);
            while (queryResponseIterator.hasNext()) {
                TargetQueryResponse response = queryResponseIterator.next();
                fetchedData += response.getSerializedSize();
                for (ProtoBuffSerializedStrand strand : response.getStrandsList()) {
                    System.out.println("Geohash: " + strand.getGeohash());
                    System.out.println("Start TS: " + strand.getStartTS());
                    System.out.println("Features: " + strand.getFeaturesList());
                    System.out.println("Observation Count: " + strand.getObservationCount());
                    /* Only need to access data container values if the
                     observation count is > 1. Otherwise you can use the feature list.*/
                    if (strand.getObservationCount() > 1) {
                        System.out.println("Min values: " + strand.getMinList());
                        System.out.println("Max values: " + strand.getMaxList());
                        System.out.println("M2 values: " + strand.getM2List());
                        System.out.println("S2 values: " + strand.getS2List());
                    }
                }
            }
            long t2 = System.currentTimeMillis();
            System.out.println("Query is complete. Elapsed time (ms): " + (t2 - t1) + ", fetched data: " + fetchedData
                               + ", download rate (MB/s): " + fetchedData * 1000 / (1204 * 1024d * (t2 - t1)));
            System.out.println("\n-----------------\n");
        }
    }
}
