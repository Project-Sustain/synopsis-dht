package sustain.synopsis.samples.client.epa.airquality;

import sustain.synopsis.samples.client.common.Location;

import java.time.LocalDateTime;

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

}
