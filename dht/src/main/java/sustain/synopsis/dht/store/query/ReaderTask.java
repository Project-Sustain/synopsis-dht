package sustain.synopsis.dht.store.query;

import com.google.protobuf.ByteString;
import org.apache.log4j.Logger;
import sustain.synopsis.dht.store.StrandStorageKey;
import sustain.synopsis.dht.store.entity.EntityStore;
import sustain.synopsis.dht.store.services.MatchingStrand;
import sustain.synopsis.dht.store.services.TargetQueryRequest;
import sustain.synopsis.dht.store.services.TargetQueryResponse;
import sustain.synopsis.storage.lsmtree.SSTableReader;
import sustain.synopsis.storage.lsmtree.TableIterator;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ReaderTask implements Runnable {

    private static final int DEFAULT_BATCH_SIZE = 1204 * 1024;
    private Logger logger = Logger.getLogger(ReaderTask.class);
    private final EntityStore entityStore;
    private final TargetQueryRequest queryRequest;
    private final QueryContainer container;
    private final int batchSize;

    public ReaderTask(EntityStore entityStore, TargetQueryRequest queryRequest, QueryContainer container,
                      int batchSize) {
        this.entityStore = entityStore;
        this.queryRequest = queryRequest;
        this.container = container;
        this.batchSize = batchSize;
    }

    public ReaderTask(EntityStore entityStore, TargetQueryRequest queryRequest, QueryContainer container) {
        this(entityStore, queryRequest, container, DEFAULT_BATCH_SIZE);
    }

    @Override
    public void run() {
        try {
            List<MatchedSSTable> matchingSSTables = entityStore.temporalQuery(queryRequest.getTemporalScope());
            if (matchingSSTables.isEmpty()) {
                container.complete();
                return;
            }
            for (MatchedSSTable matchedSSTable : matchingSSTables) {
                readSSTable(matchedSSTable);
            }
        } catch (Throwable e) { // not letting the thread get killed.
            logger.error(e.getMessage(), e);
        } finally {
            container.complete();
        }
    }

    private void readSSTable(MatchedSSTable matchedSSTable) throws IOException {
        Set<StrandStorageKey> matchingBlocks =
                matchedSSTable.getMatchedIntervals().stream().flatMap(interval -> QueryUtil.temporalLookup(matchedSSTable.getMetadata().getBlockIndex(), interval.getFrom(), interval.getTo(), false).keySet().stream()).collect(Collectors.toSet());
        SSTableReader<StrandStorageKey> reader = new SSTableReader<>(matchedSSTable.getMetadata(),
                StrandStorageKey.class);
        for (StrandStorageKey firstKey : matchingBlocks) {
            sendStrandsAsBatches(readBlock(reader, firstKey, matchedSSTable.getMatchedIntervals()));
        }
    }

    List<TableIterator.TableEntry<StrandStorageKey, byte[]>> readBlock(SSTableReader<StrandStorageKey> reader
            , StrandStorageKey firstKey, List<Interval> intervals) throws IOException {
        // read the block data and filter individual strands again
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(reader.readBlock(firstKey),
                Spliterator.ORDERED), false).filter(entry -> {
            boolean include = false;
            Interval scope = new Interval(entry.getKey().getStartTS(), entry.getKey().getEndTS());
            for (Interval interval : intervals) {
                // if one interval at-least overlaps with the scope of the Strand include it in the response.
                if (interval.isOverlapping(scope)) {
                    include = true;
                    break;
                }
            }
            return include;
        }).collect(Collectors.toList());
    }

    void sendStrandsAsBatches(List<TableIterator.TableEntry<StrandStorageKey, byte[]>> entries) {
        TargetQueryResponse response = TargetQueryResponse.newBuilder().buildPartial();
        int payloadSize = 0;
        for (TableIterator.TableEntry<StrandStorageKey, byte[]> entry : entries) {
            MatchingStrand strand =
                    MatchingStrand.newBuilder().setSpatialScope(entityStore.getEntityId()).setFromTS(entry.getKey().getStartTS()).setToTS(entry.getKey().getEndTS()).setStrand(ByteString.copyFrom(entry.getValue())).build();
            response = response.toBuilder().addStrands(strand).buildPartial();
            payloadSize += response.getSerializedSize();
            if (payloadSize > batchSize) {
                container.write(response.toBuilder().build());
                response = TargetQueryResponse.newBuilder().buildPartial();
                payloadSize = 0;
            }
        }
        if (payloadSize > 0) {
            container.write(response.toBuilder().build());
        }
    }
}
