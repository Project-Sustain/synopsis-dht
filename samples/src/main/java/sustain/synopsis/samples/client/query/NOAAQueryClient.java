package sustain.synopsis.samples.client.query;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import sustain.synopsis.dht.store.services.*;
import sustain.synopsis.ingestion.client.core.TemporalQuantizer;

import java.time.LocalDateTime;
import java.time.Month;
import java.util.Iterator;

public class NOAAQueryClient {
    public static void main(String[] args) {
        // initialize the stub
        String host = "lattice-80.cs.colostate.edu";
        int port = 9091;
        Channel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        TargetedQueryServiceGrpc.TargetedQueryServiceBlockingStub stub =
                TargetedQueryServiceGrpc.newBlockingStub(channel);

        // Temporal predicate - should be provided using epochs (UTC)
        // t >= 00:00 Jan 01, 2015
        Predicate fromPredicate =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN_OR_EQUAL).setIntegerValue(TemporalQuantizer.localDateTimeToEpoch(LocalDateTime.of(2015, Month.JANUARY, 1, 0, 0))).build();
        // t < 12:00 Jan 03, 2015
        Predicate toPredicate =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.LESS_THAN).setIntegerValue(TemporalQuantizer.localDateTimeToEpoch(LocalDateTime.of(2015, Month.JANUARY, 3, 12, 0))).build();
        // combine both predicates such that 00:00 Jan 01, 2015 =< t < 12:00 Jan 03, 2015
        Expression temporalExp =
                Expression.newBuilder().setPredicate1(fromPredicate).setCombineOp(Expression.CombineOperator.AND).setPredicate2(toPredicate).build();

        String[] geohashPrefixes = new String[]{"9y8b9", "9y8b", "9y8", "9y", "9"};

        // run queries for each of the geohashes
        for (String geohash : geohashPrefixes) {
            Predicate spatialPredicate = Predicate.newBuilder().setStringValue(geohash).build();
            // I've set the dataset id to be noaa_2015_jan during ingestion.
            TargetQueryRequest targetQueryRequest =
                    TargetQueryRequest.newBuilder().setDataset("noaa_2015_jan").addSpatialScope(spatialPredicate).setTemporalScope(temporalExp).build();

            long fetchedData = 0;
            long t1 = System.currentTimeMillis();
            Iterator<TargetQueryResponse> queryResponseIterator = stub.query(targetQueryRequest);
            while (queryResponseIterator.hasNext()) {
                TargetQueryResponse response = queryResponseIterator.next();
                fetchedData += response.getSerializedSize();
                // I've commented out the deserialization code to check the query + data transfer overhead
                /*for (MatchingStrand matchingStrand : response.getStrandsList()) {
                    System.out.println("geohash: " + matchingStrand.getSpatialScope() + ", from: " + matchingStrand
                    .getFromTS() + ", to: " + matchingStrand.getFromTS());
                    byte[] serializedStrand = matchingStrand.getStrand().toByteArray();
                    ByteArrayInputStream bais = new ByteArrayInputStream(serializedStrand);
                    SerializationInputStream sis = new SerializationInputStream(bais);
                    try {
                        sustain.synopsis.common.Strand strand = new sustain.synopsis.common.Strand(sis);
                        Path path = strand.getPath();
                        for(Vertex v : path){
                            System.out.println(v.getLabel().getName() + ":" + v.getLabel().dataToString());
                        }
                        DataContainer dataContainer = path.get(path.size()-1).getData();
                    } catch (IOException | SerializationException e) {
                        e.printStackTrace();
                    }
                }*/

            }
            long t2 = System.currentTimeMillis();
            System.out.println("Query is complete. Elapsed time (ms): " + (t2 - t1) + ", fetched data: " + fetchedData + ", download rate (MB/s): " + fetchedData * 1000 / (1204 * 1024d * (t2 - t1)));
            System.out.println("\n-----------------\n");
        }
    }
}
