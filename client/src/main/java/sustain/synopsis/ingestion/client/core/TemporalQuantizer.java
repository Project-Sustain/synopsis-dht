package sustain.synopsis.ingestion.client.core;

import java.time.*;

/**
 * Discretizes the time axis into intervals.
 */
public class TemporalQuantizer {
    private final Duration interval;
    private LocalDateTime boundary;

    public TemporalQuantizer(Duration interval) {
        this.interval = interval;
    }

    /**
     * Return the first boundary after the given timestamp such that boundary >= timestamp
     *
     * @param ts Epoch millisecond timestamp in UTC
     * @return Boundary as a epoch millisecond timestamp
     */
    public long[] getTemporalBoundaries(long ts) {
        LocalDateTime dateTime = epochToLocalDateTime(ts);
        if (boundary == null) {
            boundary = dateTime.minusHours(dateTime.getHour()).minusMinutes(dateTime.getMinute()).minusSeconds(
                    dateTime.getSecond()).minusNanos(dateTime.getNano()); // initialize to the start of the day of the first timestamp
        }
        if (boundary.minus(interval).isAfter(dateTime)) { // an out of order event arrived after advancing the boundary
            LocalDateTime tempBoundary = boundary.minus(interval); // this is a rare case. So handle it explicitly by temporary reverting back the boundary to an older value
            while (!tempBoundary.isBefore(dateTime)) {    // find the last boundary before the ts
                tempBoundary = tempBoundary.minus(interval);
            }
            return new long[]{localDateTimeToEpoch(tempBoundary.minus(interval)),
                    localDateTimeToEpoch(tempBoundary.plus(interval))};
        }
        while (boundary.isBefore(dateTime)) {   // find the first boundary after the timestamp - this also handles the case of sparse/missing timestamps
            boundary = boundary.plus(interval);
        }
        return new long[]{localDateTimeToEpoch(boundary.minus(interval)), localDateTimeToEpoch(boundary)};
    }

    public static long localDateTimeToEpoch(LocalDateTime localDateTime) {
        ZonedDateTime zdt = localDateTime.atZone(ZoneId.of("UTC"));
        return zdt.toInstant().toEpochMilli();
    }

    public static LocalDateTime epochToLocalDateTime(long startTS) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(startTS), ZoneId.of("UTC"));
    }
}
