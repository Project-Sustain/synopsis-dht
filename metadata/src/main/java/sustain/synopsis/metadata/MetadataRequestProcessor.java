package sustain.synopsis.metadata;

import java.util.concurrent.CompletableFuture;

public interface MetadataRequestProcessor {

    CompletableFuture<BinConfigurationResponse>  process(BinConfigurationRequest getRequest);
    CompletableFuture<PublishBinConfigurationResponse> process(PublishBinConfigurationRequest publishRequest);

}
