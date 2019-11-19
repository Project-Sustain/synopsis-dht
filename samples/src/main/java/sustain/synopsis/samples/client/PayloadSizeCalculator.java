package sustain.synopsis.samples.client;

import org.apache.log4j.Logger;
import sustain.synopsis.ingestion.client.core.Strand;
import sustain.synopsis.ingestion.client.core.StrandPublisher;
import sustain.synopsis.sketch.serialization.SerializationOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class PayloadSizeCalculator implements StrandPublisher {

    private AtomicLong cumulativeDataTransferred = new AtomicLong(0);
    private final Logger logger = Logger.getLogger(PayloadSizeCalculator.class);

    @Override
    public void publish(Set<Strand> strands) {
        for(Strand s: strands){
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            SerializationOutputStream sos = new SerializationOutputStream(baos);
            try {
                s.serialize(sos);
                sos.flush();
                baos.flush();
                cumulativeDataTransferred.addAndGet(baos.toByteArray().length);
            } catch (IOException e) {
                logger.error("Error during serializing strands.", e);
            } finally {
                try {
                    sos.close();
                    baos.close();
                } catch (IOException ignore) {

                }
            }
        }
    }

    public long getCumulativePayloadSize(){
        return cumulativeDataTransferred.get();
    }
}
