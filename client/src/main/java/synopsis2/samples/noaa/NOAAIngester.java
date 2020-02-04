package synopsis2.samples.noaa;

import org.apache.log4j.Logger;
import sustain.synopsis.common.Strand;
import sustain.synopsis.ingestion.client.core.TemporalQuantizer;
import sustain.synopsis.ingestion.client.geohash.GeoHash;
import sustain.synopsis.sketch.dataset.Metadata;
import sustain.synopsis.sketch.dataset.Quantizer;
import sustain.synopsis.sketch.dataset.feature.Feature;
import sustain.synopsis.sketch.graph.DataContainer;
import sustain.synopsis.sketch.graph.Path;
import sustain.synopsis.sketch.serialization.SerializationException;
import sustain.synopsis.sketch.serialization.SerializationInputStream;
import sustain.synopsis.sketch.serialization.Serializer;
import sustain.synopsis.sketch.stat.RunningStatisticsND;
import synopsis2.client.Ingester;
import synopsis2.client.IngestionConfig;

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
    private final TemporalQuantizer temporalQuantizer;
    private boolean initialized = false;

    public NOAAIngester(String baseInputDir, IngestionConfig config) {
        this.baseInputDir = baseInputDir;
        this.ingestionConfig = config;
        this.temporalQuantizer = new TemporalQuantizer(config.getTemporalGranularity());
    }

    @Override
    public void initialize() {
        inputFiles = getInputFilesInDir(baseInputDir);
        logger.info("Input file count: " + inputFiles.length);
    }

    @Override
    public boolean hasNext() {
        if(!initialized){
            initialize();
            this.initialized = true;
        }
        return recordCount > 0 || index < inputFiles.length;
    }

    @Override
    public Strand next() {
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

    private Strand parse() {
        Strand record = null;
        try {
            float lat = inStream.readFloat();
            float lon = inStream.readFloat();
            byte[] payload = inStream.readField();
            Metadata eventMetadata = Serializer.deserialize(sustain.synopsis.sketch.dataset.Metadata.class, payload);
            String stringHash = GeoHash.encode(lat, lon, ingestionConfig.getPrecision());
            long ts = eventMetadata.getTemporalProperties().getEnd();
            record = constructStrand(stringHash, ts, eventMetadata);
        } catch (IOException e) {
            logger.error("Read Error.", e);
        } catch (SerializationException e) {
            logger.error("Deserialization Error.", e);
        }
        return record;
    }

    private Strand constructStrand(String geohash, long ts, Metadata metadata) {
        List<String> features = ingestionConfig.getFeatures();
        Path path = new Path(features.size() + 2); // additional vertices for time and location

        // path: time -> feature 1 -> ..... -> feature n -> geohash (data container)
        long[] temporalBracket = temporalQuantizer.getTemporalBoundaries(ts);
        double[] values = new double[features.size()]; // skip time and location
        int i = 0;
        for (String feature : features) {
            Quantizer quantizer = ingestionConfig.getQuantizer(feature);
            Feature quantizedVal = quantizer.quantize(metadata.getAttribute(feature));
            values[i++] = metadata.getAttribute(feature).getDouble();
            path.add(new Feature(feature, quantizedVal));
        }
        // create the data container and set as the data of the last vertex
        RunningStatisticsND rsnd = new RunningStatisticsND(values);
        DataContainer container = new DataContainer(rsnd);
        path.get(path.size() - 1).setData(container);
        return new Strand(geohash, temporalBracket[0], temporalBracket[1], path);
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
            File currentFile = inputFiles[index++];
            FileInputStream fIn = new FileInputStream(currentFile);
            BufferedInputStream bIn = new BufferedInputStream(fIn);
            this.inStream = new SerializationInputStream(bIn);
            this.recordCount = inStream.readInt();
            logger.info("Ingesting data from file: " + currentFile.getAbsolutePath() + ", Record Count: " + recordCount);
        } catch (IOException e) {
            logger.error("Error opening the new file.", e);
        }
    }
}
