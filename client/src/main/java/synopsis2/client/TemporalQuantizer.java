package synopsis2.client;

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
     * Return the first boundary after the given timestamp such that boundary > timestamp
     *
     * @param ts Epoch millisecond timestamp
     * @return Boundary as a epoch millisecond timestamp
     */
    public long getBoundary(long ts) {
        LocalDateTime dateTime = epochToLocalDateTime(ts);
        if (boundary == null) {
            boundary = dateTime.minusHours(dateTime.getHour()).minusMinutes(dateTime.getMinute()).minusSeconds(
                    dateTime.getSecond()).minusNanos(dateTime.getNano()); // initialize to the start of the day of the first timestamp
        }
        if (boundary.minus(interval).isAfter(dateTime)) { // an out of order event arrived after advancing the boundary
            LocalDateTime tempBoundary = boundary.minus(interval); // this is a rare case. So handle it explicitly by temporary reverting back the boundary to an older value
            while (tempBoundary.isAfter(dateTime)) {    // find the last boundary before the ts
                tempBoundary = tempBoundary.minus(interval);
            }
            return localDateTimeToEpoch(tempBoundary.plus(interval));
        }
        while (boundary.isBefore(dateTime)) {   // find the first boundary after the timestamp - this also handles the case of sparse/missing timestamps
            boundary = boundary.plus(interval);
        }
        return localDateTimeToEpoch(boundary);
    }

    private long localDateTimeToEpoch(LocalDateTime localDateTime) {
        ZonedDateTime zdt = localDateTime.atZone(ZoneId.of("UTC"));
        return zdt.toInstant().toEpochMilli();
    }

    private LocalDateTime epochToLocalDateTime(long startTS) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(startTS), ZoneId.of("UTC"));
    }
}
