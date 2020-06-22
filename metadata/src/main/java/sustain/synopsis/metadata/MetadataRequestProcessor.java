package sustain.synopsis.metadata;

import java.util.concurrent.CompletableFuture;

public interface MetadataRequestProcessor {

    CompletableFuture<GetMetadataResponse>  process(GetMetadataRequest getRequest);
    CompletableFuture<PublishMetadataResponse> process(PublishMetadataRequest publishRequest);

}
