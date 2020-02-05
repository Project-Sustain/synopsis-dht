package sustain.synopsis.dht.store.node;

import sustain.synopsis.common.Strand;
import sustain.synopsis.dht.Context;
import sustain.synopsis.dht.NodeConfiguration;
import sustain.synopsis.dht.Util;
import sustain.synopsis.dht.journal.Logger;
import sustain.synopsis.dht.store.StorageException;
import sustain.synopsis.dht.store.entity.EntityStore;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class NodeStore {

    private org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(NodeStore.class);
    private Map<String,Map<String, EntityStore>> entityStoreMap = new HashMap<>();

    public void init() throws StorageException {
        // initialize all entity stores
        NodeConfiguration nodeConfiguration = Context.getInstance().getNodeConfig();
        String rootJournalLoc = nodeConfiguration.getRootJournalLoc();
        Logger rootLogger = new Logger(getRootJournalFileName(rootJournalLoc));

        logger.info("Starting node store initialization.");
        long nodeInitStartTS = System.currentTimeMillis();
        for (byte[] serialized : rootLogger) {
            if (serialized == null) {
                continue;
            }
            CreateEntityStoreActivity createEntityStore = new CreateEntityStoreActivity();
            try {
                createEntityStore.deserialize(serialized);
                EntityStore entityStore = new EntityStore(createEntityStore.getEntityId(),
                        createEntityStore.getEntityJournalLogLocation(), 0L, 0L);
                if (logger.isDebugEnabled()) {
                    logger.debug("Initializing entity store: " + createEntityStore.getEntityId());
                }
                long entityStoreStartTS = System.currentTimeMillis();
                entityStore.init();
                long entityStoreEndTS = System.currentTimeMillis();
                if (logger.isDebugEnabled()) {
                    logger.debug("Completed initializing entity store: " + createEntityStore.getEntityId() + ", " +
                            "elapsed time (s): " + (entityStoreEndTS - entityStoreStartTS) / (1000.0));
                }
                entityStoreMap.putIfAbsent(createEntityStore.getDataSetId(), new HashMap<>());
                entityStoreMap.get(createEntityStore.getDataSetId()).put(createEntityStore.getEntityId(), entityStore);
            } catch (IOException | StorageException e) {
                logger.error(e);
                throw new StorageException("Error initializing node store.", e);
            }
        }
        long nodeInitEndTS = System.currentTimeMillis();
        logger.info("Completed node store initialization. Elapsed time (s): " + (nodeInitEndTS - nodeInitStartTS) / 1000.0);
    }

    public void store(Strand strand) {
        // retrieve dataset and entity
        // find the appropriate entity store
        // If a new entity store is created, journal its metadata location
        // store the strand - check the status of the storage
    }

    String getRootJournalFileName(String rootJournalLoc) {
        return rootJournalLoc + File.separator + Util.getHostname() + "_root.slog";
    }
}
