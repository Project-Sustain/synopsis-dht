package synopsis2.samples.noaa;

import io.sigpipe.sing.dataset.Metadata;
import io.sigpipe.sing.dataset.analysis.Quantizer;
import io.sigpipe.sing.dataset.feature.Feature;
import io.sigpipe.sing.graph.DataContainer;
import io.sigpipe.sing.graph.Path;
import io.sigpipe.sing.serialization.SerializationException;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.Serializer;
import io.sigpipe.sing.stat.RunningStatisticsND;
import org.apache.log4j.Logger;
import synopsis2.SpatioTemporalRecord;
import synopsis2.client.Ingester;
import synopsis2.client.IngestionConfig;
import synopsis2.client.geohash.GeoHash;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

public class NOAAIngester implements Ingester {

    private static final Logger logger = Logger.getLogger(NOAAIngester.class);
    private final IngestionConfig ingestionConfig;
    private final String baseInputDir;
    private File[] inputFiles;
    private int index = 0;
    private SerializationInputStream inStream;
    private int recordCount = 0;

    public NOAAIngester(String baseInputDir, IngestionConfig config) {
        this.baseInputDir = baseInputDir;
        this.ingestionConfig = config;
    }

    @Override
    public void initialize() {
        inputFiles = getInputFilesInDir(baseInputDir);
        logger.info("Input file count: " + inputFiles.length);
    }

    @Override
    public boolean hasNext() {
        return recordCount > 0 || index < inputFiles.length;
    }

    @Override
    public SpatioTemporalRecord next() {
        if (recordCount == 0) {
            startNextFile();
        }
        if (recordCount > 0) {
            recordCount--;
            return parse();
        } else {
            return null;
        }
    }

    private SpatioTemporalRecord parse() {
        SpatioTemporalRecord record = null;
        try {
            float lat = inStream.readFloat();
            float lon = inStream.readFloat();
            byte[] payload = inStream.readField();
            Metadata eventMetadata = Serializer.deserialize(io.sigpipe.sing.dataset.Metadata.class, payload);
            String stringHash = GeoHash.encode(lat, lon, ingestionConfig.getPrecision());
            record = new SpatioTemporalRecord(stringHash, eventMetadata.getTemporalProperties().getEnd(),
                    constructStrand(stringHash, eventMetadata));
        } catch (IOException e) {
            logger.error("Read Error.", e);
        } catch (SerializationException e) {
            logger.error("Deserialization Error.", e);
        }
        return record;
    }

    private Path constructStrand(String geohash, Metadata metadata) {
        List<String> features = ingestionConfig.getFeatures();
        Path path = new Path(features.size() + 1); // additional vertex for location

        double[] values = new double[path.size() - 1]; // skip the location
        int i = 0;
        for(String feature : features){
            Quantizer quantizer = ingestionConfig.getQuantizer(feature);
            Feature quantizedVal = quantizer.quantize(metadata.getAttribute(feature));
            values[i++] = metadata.getAttribute(feature).getDouble();
            path.add(new Feature(feature, quantizedVal));
        }
        path.add(new Feature("location", geohash));
        // create the data container and set as the data of the last vertex
        RunningStatisticsND rsnd = new RunningStatisticsND(values);
        DataContainer container = new DataContainer(rsnd);
        path.get(path.size()-1).setData(container);
        return path;
    }

    private File[] getInputFilesInDir(String dataDirPath) {
        File dataDir = new File(dataDirPath);
        if (dataDir.exists()) {
            inputFiles = dataDir.listFiles((dir, name) -> name.endsWith(".mblob"));
        } else {
            inputFiles = new File[0];
        }
        return inputFiles;
    }

    private void startNextFile() {
        if (index >= inputFiles.length) {
            return;
        }
        try {
            FileInputStream fIn = new FileInputStream(inputFiles[index++]);
            BufferedInputStream bIn = new BufferedInputStream(fIn);
            this.inStream = new SerializationInputStream(bIn);
            this.recordCount = inStream.readInt();
        } catch (IOException e) {
            logger.error("Error opening the new file.", e);
        }
    }
}
