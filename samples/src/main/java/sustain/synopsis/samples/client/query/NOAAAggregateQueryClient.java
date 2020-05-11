package sustain.synopsis.samples.client.query;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import sustain.synopsis.common.CommonUtil;
import sustain.synopsis.common.ProtoBuffSerializedStrand;
import sustain.synopsis.common.Strand;
import sustain.synopsis.dht.store.services.*;
import sustain.synopsis.sketch.serialization.SerializationOutputStream;
import sustain.synopsis.sketch.stat.RunningStatisticsND;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

public class NOAAAggregateQueryClient {
    public static void main(String[] args) throws IOException {
        // initialize the stub
        String host = "lattice-185.cs.colostate.edu";
        int port = 9091;
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
        // t < 12:00 Jan 03, 2015
        Predicate toPredicate = Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.LESS_THAN)
                .setIntegerValue(CommonUtil.localDateTimeToEpoch(
                        LocalDateTime.of(2015, Month.JANUARY, 31, 23, 59))).build();
        // combine both predicates such that 00:00 Jan 01, 2015 =< t < 12:00 Jan 03, 2015
        Expression temporalExp =
                Expression.newBuilder().setPredicate1(fromPredicate).setCombineOp(Expression.CombineOperator.AND)
                        .setPredicate2(toPredicate).build();

        //String[] geohashPrefixes = new String[]{"9y8b9", "9y8b", "9y8", "9y", "9"};
        String[] geohashPrefixes = new String[]{"9x"};

        FileOutputStream fileOutputStream = new FileOutputStream("2015_Jan_month");
        SerializationOutputStream dataOutputStream = new SerializationOutputStream(fileOutputStream);

        // run queries for each of the geohashes
        Map<String, Strand> aggregatedStrands = new HashMap<>();
        for (String geohash : geohashPrefixes) {
            Predicate spatialPredicate = Predicate.newBuilder().setStringValue(geohash).build();
            // I've set the dataset id to be noaa_2015_jan during ingestion.
            TargetQueryRequest targetQueryRequest =
                    TargetQueryRequest.newBuilder().setDataset("noaa_2015_jan_1wk").addSpatialScope(spatialPredicate)
                            .setTemporalScope(temporalExp).build();

            long fetchedData = 0;
            long t1 = System.currentTimeMillis();
            Iterator<TargetQueryResponse> queryResponseIterator = stub.query(targetQueryRequest);
            while (queryResponseIterator.hasNext()) {
                TargetQueryResponse response = queryResponseIterator.next();
                fetchedData += response.getSerializedSize();
                for (ProtoBuffSerializedStrand strand : response.getStrandsList()) {
                    sustain.synopsis.common.Strand s = CommonUtil.protoBuffToStrand(strand);
                    if (aggregatedStrands.containsKey(s.getKey())) {
                        aggregatedStrands.get(s.getKey()).merge(s);
                    } else {
                        aggregatedStrands.put(s.getKey(), s);
                    }
                }

            }
            long t2 = System.currentTimeMillis();
            System.out.println("Query is complete. Elapsed time (ms): " + (t2 - t1) + ", fetched data: " + fetchedData
                    + ", download rate (MB/s): " + fetchedData * 1000 / (1204 * 1024d * (t2 - t1)));
            System.out.println("\n-----------------\n");
        }
        System.out.println("---- Aggregated Strands -----");
        System.out.println("Total Count: " + aggregatedStrands.size());
        for (Strand s : aggregatedStrands.values()) {
            System.out.println("--");
            System.out.println("Geohash: " + s.getGeohash());
            System.out.println("From Timestamp: " + s.getFromTimeStamp());
            System.out.println("Path: " + s.getPath().stream().map(vertex -> vertex.getLabel().getDouble() + ",")
                    .collect(Collectors.joining()));
            RunningStatisticsND stats = s.getPath().get(s.getPath().size() - 1).getData().statistics;
            System.out.println("Observation Count: " + stats.count());
            System.out.println("Mean: " + Arrays.toString(stats.means()));
            System.out.println("Std. Dev.: " + Arrays.toString(stats.stds()));
            System.out.println("Mins: " + Arrays.toString(stats.mins()));
            System.out.println("Maxes: " + Arrays.toString(stats.maxes()));

            s.serialize(dataOutputStream);
        }

        dataOutputStream.close();

        /*
            Geohash: 9x5gf
            From Timestamp: 1420761600000
            Path: 247.01815795898438,91.5853271484375,-1.2860326766967773,
            Observation Count: 1
            Mean: [256.2893371582031, 97.0, 3.619378089904785]
            Std. Dev.: [NaN, NaN, NaN]
            Mins: [256.2893371582031, 97.0, 3.619378089904785]
            Maxes: [256.2893371582031, 97.0, 3.619378089904785]
            */
    }
}
