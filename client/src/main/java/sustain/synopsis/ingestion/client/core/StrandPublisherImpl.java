package sustain.synopsis.ingestion.client.core;

import sustain.synopsis.common.Strand;

import java.util.Collection;

public class StrandPublisherImpl implements StrandPublisher {

    long countSum = 0;
    long strandCount = 0;

    @Override
    public void publish(Collection<Strand> strands) {
        strandCount += strands.size();
        for (Strand strand : strands) {
            countSum += strand.getPath().get(0).getData().statistics.count();
//            System.out.printf("%s %d %d %d\n", strand.getGeohash(), strand.getFromTimeStamp(), strand.getToTimestamp(), strand.getPath().get(0).getData().statistics.count());
        }
    }

    public double getCountAvg() {
        return (double) countSum / strandCount;
    }
}
