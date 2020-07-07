package sustain.synopsis.samples.client.epa.airquality;

import sustain.synopsis.samples.client.common.Location;
import sustain.synopsis.sketch.util.Geohash;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class HourlyDataEntry {

    String siteId;
    Location location;
    LocalDateTime dateTime;
    String parameterName;
    String methodCode;
    float measurement;

    public HourlyDataEntry(String siteId, Location location, LocalDateTime dateTime, String parameterName, String methodCode, float measurement) {
        this.siteId = siteId;
        this.location = location;
        this.dateTime = dateTime;
        this.parameterName = parameterName;
        this.methodCode = methodCode;
        this.measurement = measurement;
    }

    String getKey() {
        String geohash = Geohash.encode(
                location.latitude,
                location.longitude,
                AirQualityClient.AIR_QUALITY_GEOHASH_LENGTH
        );
        return geohash+dateTime.toEpochSecond(ZoneOffset.UTC);
    }

}
