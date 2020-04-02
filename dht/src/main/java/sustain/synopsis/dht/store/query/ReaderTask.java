package sustain.synopsis.dht.store.query;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.log4j.Logger;
import sustain.synopsis.common.ProtoBuffSerializedStrand;
import sustain.synopsis.dht.store.StrandStorageKey;
import sustain.synopsis.dht.store.entity.EntityStore;
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
    private final EntityStore entityStore;
    private final TargetQueryRequest queryRequest;
    private final QueryContainer container;
    private final int batchSize;
    private Logger logger = Logger.getLogger(ReaderTask.class);

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
        if (logger.isDebugEnabled()) {
            logger.debug("Reader task started for entity: " + entityStore.getEntityId());
        }
        try {
            List<MatchedSSTable> matchingSSTables = entityStore.temporalQuery(queryRequest.getTemporalScope());
            if (logger.isDebugEnabled()) {
                logger.debug("Number of matching SSTables" + matchingSSTables.size());
            }
            if (!matchingSSTables.isEmpty()) {
                for (MatchedSSTable matchedSSTable : matchingSSTables) {
                    readSSTable(matchedSSTable);
                }
            }
        } catch (Throwable e) { // not letting the thread get killed.
            logger.error(e.getMessage(), e);
        } finally {
            container.complete();
        }
    }

    void readSSTable(MatchedSSTable matchedSSTable) throws IOException {
        long t1 = System.currentTimeMillis();
        Set<StrandStorageKey> matchingBlocks = matchedSSTable.getMatchedIntervals().stream().flatMap(
                interval -> QueryUtil.temporalLookup(matchedSSTable.getMetadata().getBlockIndex(), interval.getFrom(),
                                                     interval.getTo(), false).keySet().stream())
                                                             .collect(Collectors.toSet());
        SSTableReader<StrandStorageKey> reader = null;
        try {
            reader = new SSTableReader<>(matchedSSTable.getMetadata(), StrandStorageKey.class);
            for (StrandStorageKey firstKey : matchingBlocks) {
                sendStrandsAsBatches(readBlock(reader, firstKey, matchedSSTable.getMatchedIntervals()));
            }
            long t2 = System.currentTimeMillis();
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Time spent on processing the SSTable (" + matchedSSTable.getMetadata().getPath() + "): (ms)"
                        + (t2 - t1));
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }

    List<TableIterator.TableEntry<StrandStorageKey, byte[]>> readBlock(SSTableReader<StrandStorageKey> reader,
                                                                       StrandStorageKey firstKey,
                                                                       List<Interval> intervals) throws IOException {
        // read the block data and filter individual strands again
        long t1 = System.currentTimeMillis();
        List<TableIterator.TableEntry<StrandStorageKey, byte[]>> result = StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(reader.readBlock(firstKey), Spliterator.ORDERED), false)
                .filter(entry -> {
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
        long t2 = System.currentTimeMillis();
        if (logger.isDebugEnabled()) {
            logger.debug("Time spent on reading a block(ms): " + (t2 - t1));
        }
        return result;
    }

    void sendStrandsAsBatches(List<TableIterator.TableEntry<StrandStorageKey, byte[]>> entries)
            throws InvalidProtocolBufferException {
        long t1 = System.currentTimeMillis();
        TargetQueryResponse response = TargetQueryResponse.newBuilder().buildPartial();
        int payloadSize = 0;
        for (TableIterator.TableEntry<StrandStorageKey, byte[]> entry : entries) {
            ProtoBuffSerializedStrand strand =
                    ProtoBuffSerializedStrand.newBuilder().mergeFrom(entry.getValue()).build();
            response = response.toBuilder().addStrands(strand).buildPartial();
            payloadSize += entry.getValue().length;
            if (payloadSize >= batchSize) {
                container.write(response.toBuilder().build());
                response = TargetQueryResponse.newBuilder().buildPartial();
                payloadSize = 0;
            }
        }
        if (payloadSize > 0) {
            container.write(response.toBuilder().build());
        }
        long t2 = System.currentTimeMillis();
        if (logger.isDebugEnabled()) {
            logger.debug("Time spent on sending the response back: " + (t2 - t1));
        }
    }
}
