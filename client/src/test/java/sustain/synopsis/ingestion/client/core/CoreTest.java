package sustain.synopsis.ingestion.client.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Month;

class CoreTest {
    @Test
    void testTemporalQuantizer() {
        TemporalQuantizer quantizer = new TemporalQuantizer(Duration.ofHours(1));
        LocalDateTime ts1 = LocalDateTime.of(2019, Month.JANUARY, 12, 6, 21, 30);
        long[] returned = quantizer.getTemporalBoundaries(TemporalQuantizer.localDateTimeToEpoch(ts1));
        long expectedUpperBoundary = TemporalQuantizer.localDateTimeToEpoch(LocalDateTime.of(2019, Month.JANUARY, 12,
                7, 0, 0));
        long expectedLowerBoundary = TemporalQuantizer.localDateTimeToEpoch(LocalDateTime.of(2019, Month.JANUARY, 12,
                6, 0, 0));
        Assertions.assertEquals(expectedLowerBoundary, returned[0]);
        Assertions.assertEquals(expectedUpperBoundary, returned[1]);

        // simulate an out of order record - now the boundary is advanced to 07:00
        ts1 = LocalDateTime.of(2019, Month.JANUARY, 12, 3, 21, 30);
        returned = quantizer.getTemporalBoundaries(TemporalQuantizer.localDateTimeToEpoch(ts1));
        expectedUpperBoundary = TemporalQuantizer.localDateTimeToEpoch(LocalDateTime.of(2019, Month.JANUARY, 12, 4, 0, 0));
        Assertions.assertEquals(expectedUpperBoundary, returned[1]);

        // simulate a sparse observation
        ts1 = LocalDateTime.of(2019, Month.JANUARY, 12, 17, 21, 30);
        returned = quantizer.getTemporalBoundaries(TemporalQuantizer.localDateTimeToEpoch(ts1));
        expectedUpperBoundary = TemporalQuantizer.localDateTimeToEpoch(LocalDateTime.of(2019, Month.JANUARY, 12, 18, 0, 0));
        Assertions.assertEquals(expectedUpperBoundary, returned[1]);

        // check when the ts = boundaries
        ts1 = LocalDateTime.of(2019, Month.JANUARY, 12, 18, 0, 0);
        returned = quantizer.getTemporalBoundaries(TemporalQuantizer.localDateTimeToEpoch(ts1));
        expectedUpperBoundary = TemporalQuantizer.localDateTimeToEpoch(LocalDateTime.of(2019, Month.JANUARY, 12, 18, 0, 0));
        Assertions.assertEquals(expectedUpperBoundary, returned[1]);
    }
}
