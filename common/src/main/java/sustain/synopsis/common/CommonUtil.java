package sustain.synopsis.common;

import sustain.synopsis.sketch.dataset.feature.Feature;
import sustain.synopsis.sketch.graph.DataContainer;
import sustain.synopsis.sketch.graph.Path;
import sustain.synopsis.sketch.stat.RunningStatisticsND;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CommonUtil {
    public static ProtoBuffSerializedStrand strandToProtoBuff(Strand strand) {
        ProtoBuffSerializedStrand protoBuffStrand =
                ProtoBuffSerializedStrand.newBuilder().setGeohash(strand.getGeohash())
                                         .setStartTS(strand.getFromTimeStamp()).buildPartial();
        ProtoBuffSerializedStrand.Builder builder = protoBuffStrand.toBuilder();
        Path path = strand.getPath();
        builder.addAllFeatures(path.stream().map(v -> v.getLabel().getDouble()).collect(Collectors.toList()));
        RunningStatisticsND statistics = path.get(path.size() - 1).getData().statistics;
        builder.setObservationCount(statistics.count());
        /* Copy the mean values even in the case of n = 1 because they are non discretized values.
        With this approach, we can correctly merge strands with single observations.
        */
        builder.addAllMean(Arrays.stream(statistics.means()).boxed().collect(Collectors.toList()));
        if (statistics.count() > 1) {
            builder.addAllM2(Arrays.stream(statistics.m2()).boxed().collect(Collectors.toList()));
            builder.addAllMax(Arrays.stream(statistics.maxes()).boxed().collect(Collectors.toList()));
            builder.addAllMin(Arrays.stream(statistics.mins()).boxed().collect(Collectors.toList()));
            builder.addAllS2(Arrays.stream(statistics.ss()).boxed().collect(Collectors.toList()));
        }
        return builder.build();
    }

    public static Strand protoBuffToStrand(ProtoBuffSerializedStrand strand) {
        List<Double> featuresList = strand.getFeaturesList();
        Path path = new Path(featuresList.size());
        featuresList.stream().map(Feature::new).forEach(path::add);
        // set the data container
        RunningStatisticsND statisticsND;
        if (strand.getObservationCount() > 1) {
            statisticsND = new RunningStatisticsND(featuresList.size());
            statisticsND.setMean(listToArray(strand.getMeanList()));
            statisticsND.setM2(listToArray(strand.getM2List()));
            statisticsND.setMax(listToArray(strand.getMaxList()));
            statisticsND.setMin(listToArray(strand.getMinList()));
            statisticsND.setSs(listToArray(strand.getS2List()));
        } else {
            // in case of single observation Strands mean values are always non discretized data
            statisticsND = new RunningStatisticsND(listToArray(strand.getMeanList()));
        }
        statisticsND.setN(strand.getObservationCount());
        path.get(path.size() - 1).setData(new DataContainer(statisticsND));
        // We do not keep the endTS in the protobuff serialized strand because it can get resolved using
        // the schema
        return new Strand(strand.getGeohash(), strand.getStartTS(), strand.getStartTS(), path);
    }

    private static double[] listToArray(List<Double> list) {
        double[] array = new double[list.size()];
        IntStream.range(0, list.size()).forEach(i -> array[i] = list.get(i));
        return array;
    }

    public static long localDateTimeToEpoch(LocalDateTime localDateTime) {
        ZonedDateTime zdt = localDateTime.atZone(ZoneId.of("UTC"));
        return zdt.toInstant().toEpochMilli();
    }

    public static LocalDateTime epochToLocalDateTime(long startTS) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(startTS), ZoneId.of("UTC"));
    }
}
