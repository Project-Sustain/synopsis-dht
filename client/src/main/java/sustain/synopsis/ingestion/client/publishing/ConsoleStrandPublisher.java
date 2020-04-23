package sustain.synopsis.ingestion.client.publishing;

import org.apache.log4j.Logger;
import sustain.synopsis.common.CommonUtil;
import sustain.synopsis.common.Strand;
import sustain.synopsis.sketch.graph.DataContainer;
import sustain.synopsis.sketch.graph.Path;
import sustain.synopsis.sketch.graph.Vertex;

/**
 * Implementation of {@link StrandPublisher} for testing. Simply prints the completed
 * strands to the console.
 */
public class ConsoleStrandPublisher implements StrandPublisher {

    private final Logger logger = Logger.getLogger(ConsoleStrandPublisher.class);

    long totalPublished = 0;

    public void publish(long messageId, Iterable<Strand> strands) {

        for(Strand strand : strands) {
            logger.info("Published strand. Geohash: " + strand.getGeohash() +
                        ", from: " + CommonUtil.epochToLocalDateTime(strand.getFromTimeStamp()) +
                        ", to: " + CommonUtil.epochToLocalDateTime(strand.getToTimestamp()));
            StringBuilder stringBuilder = new StringBuilder();
            Path path = strand.getPath();
            for(Vertex v : path){
                stringBuilder.append(v.getLabel().getName() + "=" + v.getLabel().getDouble()).append(",");
            }
            DataContainer container = path.get(path.size()-1).getData();
            stringBuilder.append("count=").append(container.statistics.count());
            logger.info("Path: " + stringBuilder.toString());
            totalPublished++;
        }
    }

    @Override
    public long getTotalStrandsPublished() {
        return totalPublished;
    }
}
