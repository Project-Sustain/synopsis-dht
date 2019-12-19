package sustain.synopsis.samples.client;

import org.apache.log4j.Logger;
import sustain.synopsis.ingestion.client.core.StrandPublisher;
import sustain.synopsis.ingestion.client.core.TemporalQuantizer;
import sustain.synopsis.sketch.graph.DataContainer;
import sustain.synopsis.sketch.graph.Path;
import sustain.synopsis.sketch.graph.Vertex;
import synopsis2.common.kafka.Strand;

import java.util.Set;

/**
 * Implementation of {@link StrandPublisher} for testing. Simply prints the completed
 * strands to the console.
 */
public class ConsoleStrandPublisher implements StrandPublisher {

    private final Logger logger = Logger.getLogger(ConsoleStrandPublisher.class);

    @Override
    public void publish(Set<Strand> strands) {
        for(Strand strand : strands) {
            logger.info("Published strand. Geohash: " + strand.getGeohash() +
                    ", from: " + TemporalQuantizer.epochToLocalDateTime(strand.getFromTimeStamp()) +
                    ", to: " + TemporalQuantizer.epochToLocalDateTime(strand.getToTimestamp()));
            StringBuilder stringBuilder = new StringBuilder();
            Path path = strand.getPath();
            for(Vertex v : path){
                stringBuilder.append(v.getLabel().getName() + "=" + v.getLabel().getDouble()).append(",");
            }
            DataContainer container = path.get(path.size()-1).getData();
            stringBuilder.append("count=").append(container.statistics.count());
            logger.info("Path: " + stringBuilder.toString());
        }
    }
}
