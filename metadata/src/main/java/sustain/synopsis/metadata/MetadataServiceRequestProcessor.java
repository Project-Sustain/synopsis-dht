package sustain.synopsis.metadata;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class MetadataServiceRequestProcessor implements MetadataRequestProcessor {

    final File journalFile;
    final Map<String, Map<Long,ProtoBuffSerializedSessionMetadata>> datasetSessionsMap = new HashMap<>();

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

    private static ProtoBuffSerializedBinConfiguration getSerializedBinConfigFromString(String s) {
        ProtoBuffSerializedBinConfiguration.Builder binConfigBuilder = ProtoBuffSerializedBinConfiguration.newBuilder();
        String[] splits = s.split(",");
        binConfigBuilder.setFeatureName(splits[0]);
        for (int i = 1; i < splits.length; i++) {
            binConfigBuilder.addValues(Float.parseFloat(splits[i]));
        }
        return binConfigBuilder.build();
    }

    private void readLine(String line) {
        String[] splits = line.split("\t");
        String datasetId = splits[0];
        long sessionId = Long.parseLong(splits[1]);

        ProtoBuffSerializedSessionMetadata.Builder sessionMetadataBuilder =  ProtoBuffSerializedSessionMetadata.newBuilder()
                .setSessionId(sessionId);
        for (int i = 2; i < splits.length; i++) {
            sessionMetadataBuilder.addBinConfiguration(getSerializedBinConfigFromString(splits[i]));
        }

        datasetSessionsMap.computeIfAbsent(datasetId, k -> new HashMap<>())
                .put(sessionId, sessionMetadataBuilder.build());
    }

    @Override
    public CompletableFuture<GetMetadataResponse> process(GetMetadataRequest getRequest) {
        CompletableFuture<GetMetadataResponse> future = new CompletableFuture<>();

        GetMetadataResponse.Builder responseBuilder = GetMetadataResponse.newBuilder();
        for (DatasetSessions datasetSessions : getRequest.getDatasetSessionsList()) {
            responseBuilder.addDatasetMetadata(getDatasetMetadata(datasetSessions));
        }

        future.complete(responseBuilder.build());
        return future;
    }

    private ProtoBuffSerializedDatasetMetadata getDatasetMetadata(DatasetSessions datasetSessions) {
            ProtoBuffSerializedDatasetMetadata.Builder datasetBinsBuilder = ProtoBuffSerializedDatasetMetadata.newBuilder()
                    .setDatasetId(datasetSessions.getDatasetId());

            Map<Long, ProtoBuffSerializedSessionMetadata> sessionMap = datasetSessionsMap.get(datasetSessions.getDatasetId());
            for (long sessionId : datasetSessions.getSessionIdList()) {
                datasetBinsBuilder.addSessionMetadata(sessionMap.get(sessionId));
            }
            return datasetBinsBuilder.build();
    }

    @Override
    public CompletableFuture<PublishMetadataResponse> process(PublishMetadataRequest publishRequest) {
        CompletableFuture<PublishMetadataResponse> future = new CompletableFuture<>();
        PublishMetadataResponse.Builder responseBuilder = PublishMetadataResponse.newBuilder();

        Map<Long, ProtoBuffSerializedSessionMetadata> binConfigurationMap = this.datasetSessionsMap.computeIfAbsent(publishRequest.getDatasetId(), k -> new HashMap<>());
        ProtoBuffSerializedSessionMetadata sessionMetadata = publishRequest.getSessionMetadata();


        if (binConfigurationMap.containsKey(sessionMetadata.getSessionId())) {
            future.completeExceptionally(new Throwable("binConfiguration already exists for datasetId and sessionId"));
            return future;
        }

        try {
            logBinConfiguration(publishRequest.getDatasetId(), sessionMetadata);
            binConfigurationMap.put(sessionMetadata.getSessionId(), sessionMetadata);
            future.complete(responseBuilder.setStatus(true).build());

        } catch (IOException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    private static String getStringForBinConfig(ProtoBuffSerializedBinConfiguration binConfig) {
        StringBuilder sb = new StringBuilder();
        sb.append(binConfig.getFeatureName());
        for (float value : binConfig.getValuesList()) {
            sb.append(",").append(value);
        }
        return sb.toString();
    }

    private static String getStringForEntry(String datasetId, ProtoBuffSerializedSessionMetadata sessionMetadata) {
        StringBuilder sb = new StringBuilder();
        sb.append(datasetId).append("\t").append(sessionMetadata.getSessionId()).append("\t");

        List<ProtoBuffSerializedBinConfiguration> binConfigList = sessionMetadata.getBinConfigurationList();
        if (!binConfigList.isEmpty()) {
            int i = 0;
            for (; i < binConfigList.size()-1; i++) {
                sb.append(getStringForBinConfig(binConfigList.get(i)));
                sb.append("\t");
            }
            sb.append(getStringForBinConfig(binConfigList.get(i)));
        }

        sb.append("\n");
        return sb.toString();
    }

    private void logBinConfiguration(String datasetId, ProtoBuffSerializedSessionMetadata sessionMetadata) throws IOException {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(journalFile, true))) {
            bw.write(getStringForEntry(datasetId, sessionMetadata));
        }
    }

}
