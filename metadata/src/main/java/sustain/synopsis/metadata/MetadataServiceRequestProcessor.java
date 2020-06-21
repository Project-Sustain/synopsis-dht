package sustain.synopsis.metadata;

import java.io.*;
import java.nio.Buffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class MetadataServiceRequestProcessor implements MetadataRequestProcessor {

    final File journalFile;
    final Map<String, Map<Long,String>> binConfigurationMap = new HashMap<>();

    public MetadataServiceRequestProcessor(String journalLoc) {
        this.journalFile = new File(journalLoc);
        readJournalFile();
    }

    private void readJournalFile() {
        try {
            if (journalFile.exists()) {
                try (BufferedReader br = new BufferedReader(new FileReader(journalFile))){
                    br.lines().forEach(this::readLine);
                }
            } else {
                journalFile.createNewFile();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void readLine(String line) {
        String[] splits = line.split("\t",3);

        String datasetId = splits[0];
        long sessionId = Long.parseLong(splits[1]);
        String binConfig = splits[2];

        binConfigurationMap.computeIfAbsent(datasetId, k -> new HashMap<>())
                .put(sessionId, binConfig);
    }

    @Override
    public CompletableFuture<BinConfigurationResponse> process(BinConfigurationRequest getRequest) {
        CompletableFuture<BinConfigurationResponse> future = new CompletableFuture<>();

        BinConfigurationResponse.Builder responseBuilder = BinConfigurationResponse.newBuilder();
        for (DatasetSessions datasetSessions : getRequest.getDatasetSessionList()) {
            processDatasetSessions(datasetSessions, responseBuilder);
        }

        future.complete(responseBuilder.build());
        return future;
    }

    private void processDatasetSessions(DatasetSessions datasetSessions, BinConfigurationResponse.Builder responseBuilder) {
            DatasetBins.Builder datasetBinsBuilder = DatasetBins.newBuilder()
                    .setDatasetId(datasetSessions.getDatasetId());

            Map<Long, String> sessionMap = binConfigurationMap.get(datasetSessions.getDatasetId());
            for (long sessionId : datasetSessions.getSessionIdList()) {
                datasetBinsBuilder.addBinConfigurations(
                        BinConfiguration.newBuilder()
                                .setSessionId(sessionId)
                                .setBinConfiguration(sessionMap.get(sessionId))
                                .build()
                );
            }
            responseBuilder.addDatasetBins(datasetBinsBuilder.build());
    }

    @Override
    public CompletableFuture<PublishBinConfigurationResponse> process(PublishBinConfigurationRequest publishRequest) {
        CompletableFuture<PublishBinConfigurationResponse> future = new CompletableFuture<>();
        PublishBinConfigurationResponse.Builder responseBuilder = PublishBinConfigurationResponse.newBuilder();

        Map<Long, String> binConfigurationMap = this.binConfigurationMap.computeIfAbsent(publishRequest.getDatasetId(), k -> new HashMap<>());
        if (binConfigurationMap.containsKey(publishRequest.getSessionId())) {
            future.completeExceptionally(new Throwable("binConfiguration already exists for datasetId and sessionId"));
            return future;
        }

        try {
            logBinConfiguration(publishRequest.getDatasetId(), publishRequest.getSessionId(), publishRequest.getBinConfiguration());
            binConfigurationMap.put(publishRequest.getSessionId(), publishRequest.getBinConfiguration());
            future.complete(responseBuilder.setStatus(true).build());

        } catch (IOException e) {
            future.completeExceptionally(e);
        }
        return future;

    }

    private void logBinConfiguration(String datasetId, long sessionId, String binConfiguration) throws IOException {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(journalFile, true))) {
            String toWrite = datasetId+"\t"+sessionId+"\t"+binConfiguration+"\n";
            bw.write(toWrite);
        }
    }

}
