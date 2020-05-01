package sustain.synopsis.benchmarks.dht.querythroughput;

import sustain.synopsis.common.ProtoBuffSerializedStrand;
import sustain.synopsis.dht.services.query.QueryResponseHandler;
import sustain.synopsis.dht.store.services.TargetQueryResponse;

import java.util.concurrent.atomic.AtomicLong;

class DummyQueryResponseHandler implements QueryResponseHandler {
    private AtomicLong totalQueryResponseSize = new AtomicLong(0);
    private AtomicLong totalStrandCount = new AtomicLong(0);

    @Override
    public void handleResponse(TargetQueryResponse targetQueryResponse) {
        for (ProtoBuffSerializedStrand strand : targetQueryResponse.getStrandsList()) {
            totalQueryResponseSize.addAndGet(strand.getSerializedSize());
        }
        totalStrandCount.addAndGet(targetQueryResponse.getStrandsCount());
    }

    long getResponseSize() {
        return totalQueryResponseSize.get();
    }

    long getStrandCount(){
        return totalStrandCount.get();
    }
}
