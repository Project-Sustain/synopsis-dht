package sustain.synopsis.metadata;

public class MetadataServiceTest {

//    static BinConfigurationRequest getRequest = BinConfigurationRequest.newBuilder()
//            .addDatasetSession(
//                    DatasetSessions.newBuilder()
//                    .setDatasetId("usgs-stream-flow")
//                    .addSessionId(1L)
//            )
//            .build();
//
//    static BinConfigurationRequest getRequest2 = BinConfigurationRequest.newBuilder()
//            .addDatasetSession(
//                    DatasetSessions.newBuilder()
//                            .setDatasetId("usgs-stream-flow-2")
//                            .addSessionId(123)
//            )
//            .build();
//
//    static PublishBinConfigurationRequest publishRequest = PublishBinConfigurationRequest.newBuilder()
//            .setDatasetId("usgs-stream-flow-2")
//            .setSessionId(123)
//            .setBinConfiguration("myFeature,0.0,1.0,2.0,3.0")
//            .build();
//
//    static String datasetBinsToString(DatasetBins datasetBins) {
//        StringBuilder sb = new StringBuilder();
//        sb.append(datasetBins.getDatasetId()+"\n");
//        for (BinConfiguration binConfig : datasetBins.getBinConfigurationsList()) {
//            sb.append(binConfig.getSessionId()+"\t"+binConfig.getBinConfiguration()+"\n");
//        }
//        return sb.toString();
//    }
//
//    public static void main(String[] args) {
//        String journalLoc = args[0];
//        MetadataRequestProcessor requestProcessor = new MetadataServiceRequestProcessor(journalLoc);
//
//        requestProcessor.process(getRequest).thenAccept(resp -> {
//            for (DatasetBins datasetBins : resp.getDatasetBinsList()) {
//                System.out.println(datasetBinsToString(datasetBins));
//            }
//        }).exceptionally(t -> {
//            System.out.println(t.getMessage());
//            return null;
//        });
//
//
//        requestProcessor.process(publishRequest).thenAccept(resp -> {
//            System.out.println(resp.getStatus());
//        }).exceptionally(t -> {
//            System.out.println(t.getMessage());
//            return null;
//        });
//
//        requestProcessor.process(getRequest2).thenAccept(resp -> {
//            for (DatasetBins datasetBins : resp.getDatasetBinsList()) {
//                System.out.println(datasetBinsToString(datasetBins));
//            }
//        }).exceptionally(t -> {
//            System.out.println(t.getMessage());
//            return null;
//        });
//    }

}
