package sustain.synopsis.dht.services.query;

import org.apache.log4j.Logger;
import sustain.synopsis.common.ProtoBuffSerializedStrand;
import sustain.synopsis.dht.store.StrandStorageKey;
import sustain.synopsis.dht.store.StrandStorageValue;
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

    /**
     * Aggregate data from multiple reads into a single TargetQueryResponse Reduces the number of responses sent by
     * increasing the size of a single message to improve the network I/O.
     */
    class TargetQueryResponseWrapper {
        private final long batchSizeLimit;
        private TargetQueryResponse.Builder queryResponseBuilder;
        private long responseSize;

        TargetQueryResponseWrapper(long batchSizeLimit) {
            this.batchSizeLimit = batchSizeLimit;
            this.queryResponseBuilder = TargetQueryResponse.newBuilder();
            this.responseSize = 0;
        }

        void addProtoBuffSerializedStrand(ProtoBuffSerializedStrand strand) {
            this.queryResponseBuilder.addStrands(strand);
            responseSize += strand.getSerializedSize();
            if (responseSize >= batchSizeLimit) {
                publish();
            }
        }

        void publish() {
            if (responseSize > 0) {
                container.write(queryResponseBuilder.build());
                this.queryResponseBuilder.clear();
                this.responseSize = 0;
            }
        }

        void close() {
            publish();
        }
    }

    private Logger logger = Logger.getLogger(ReaderTask.class);
    private static final int DEFAULT_BATCH_SIZE = 4 * 1024;
    private final TargetQueryRequest queryRequest;
    private final QueryContainer container;
    private final TargetQueryResponseWrapper responseWrapper;

    public ReaderTask(TargetQueryRequest queryRequest, QueryContainer container, int batchSize) {
        this.queryRequest = queryRequest;
        this.container = container;
        this.responseWrapper = new TargetQueryResponseWrapper(batchSize);
    }

    public ReaderTask(TargetQueryRequest queryRequest, QueryContainer container) {
        this(queryRequest, container, DEFAULT_BATCH_SIZE);
    }

    @Override
    public void run() {
        boolean successful = true;
        try {
            EntityStore entityStore;
            while ((entityStore = container.getNextTask()) != null) {
                List<MatchedSSTable> matchingSSTables = entityStore.temporalQuery(queryRequest.getTemporalScope());
                if (logger.isDebugEnabled()) {
                    logger.debug("Entity store: " + entityStore.getEntityId() + "Number of matching SSTables"
                                 + matchingSSTables.size());
                }
                for (MatchedSSTable matchedSSTable : matchingSSTables) {
                    readSSTable(matchedSSTable);
                }
            }
            responseWrapper.close();
        } catch (Throwable e) { // not letting the thread get killed.
            logger.error(e.getMessage(), e);
            successful = false;
        } finally {
            container.reportReaderTaskComplete(successful);
        }
    }

    void readSSTable(MatchedSSTable matchedSSTable) throws IOException {
        long t1 = System.currentTimeMillis();
        Set<StrandStorageKey> matchingBlocks = matchedSSTable.getMatchedIntervals().stream().flatMap(
                interval -> QueryUtil.temporalLookup(matchedSSTable.getMetadata().getBlockIndex(), interval.getFrom(),
                                                     interval.getTo(), false).keySet().stream())
                                                             .collect(Collectors.toSet());
        SSTableReader<StrandStorageKey, StrandStorageValue> reader = null;
        try {
            reader =
                    new SSTableReader<>(matchedSSTable.getMetadata(), StrandStorageKey.class, StrandStorageValue.class);
            for (StrandStorageKey firstKey : matchingBlocks) {
                appendToResponse(readBlock(reader, firstKey, matchedSSTable.getMatchedIntervals()));
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

    List<TableIterator.TableEntry<StrandStorageKey, StrandStorageValue>> readBlock(
            SSTableReader<StrandStorageKey, StrandStorageValue> reader, StrandStorageKey firstKey,
            List<Interval> intervals) throws IOException {
        // read the block data and filter individual strands again
        long t1 = System.currentTimeMillis();
        List<TableIterator.TableEntry<StrandStorageKey, StrandStorageValue>> result = StreamSupport
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

    void appendToResponse(List<TableIterator.TableEntry<StrandStorageKey, StrandStorageValue>> entries)
            throws IOException {
        for (TableIterator.TableEntry<StrandStorageKey, StrandStorageValue> entry : entries) {
            entry.getValue().getProtoBuffSerializedStrands().forEach(responseWrapper::addProtoBuffSerializedStrand);
        }
    }
}