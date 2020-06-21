package sustain.synopsis.proxy.metadata;


import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import sustain.synopsis.dht.Context;
import sustain.synopsis.dht.NodeConfiguration;
import sustain.synopsis.metadata.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ProxyMetadataRequestProcessor implements MetadataRequestProcessor {

    private MetadataServiceGrpc.MetadataServiceFutureStub metadataStub = null;
    private ExecutorService responseExecutorService = Executors.newFixedThreadPool(4);

    @Override
    public CompletableFuture<BinConfigurationResponse> process(BinConfigurationRequest getRequest) {
        CompletableFuture<BinConfigurationResponse> future = new CompletableFuture<>();

        ListenableFuture<BinConfigurationResponse> listenableFuture =
                getMetadataStub().getBinConfigurationRequest(getRequest);

        listenableFuture.addListener(() -> {
            try {
                BinConfigurationResponse response = listenableFuture.get();
                future.complete(response);

            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        }, responseExecutorService);

        return new CompletableFuture<>();
    }

    @Override
    public CompletableFuture<PublishBinConfigurationResponse> process(PublishBinConfigurationRequest publishRequest) {
        CompletableFuture<PublishBinConfigurationResponse> future = new CompletableFuture<>();

        ListenableFuture<PublishBinConfigurationResponse> listenableFuture =
                getMetadataStub().publishBinConfiguration(publishRequest);

        listenableFuture.addListener(() -> {
            try {
                PublishBinConfigurationResponse response = listenableFuture.get();
                future.complete(response);

            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        }, responseExecutorService);

        return new CompletableFuture<>();
    }

    private MetadataServiceGrpc.MetadataServiceFutureStub getMetadataStub() {
        if (metadataStub != null) {
            return metadataStub;
        }
        synchronized (this) {
            if (metadataStub != null) {
                return metadataStub;
            }
            NodeConfiguration nc = Context.getInstance().getNodeConfig();

            Channel channel =
                    ManagedChannelBuilder.forAddress(nc.getMetadataHost(), nc.getMetadataServicePort()).usePlaintext().build();

            metadataStub = MetadataServiceGrpc.newFutureStub(channel);
            return metadataStub;
        }
    }

}
