package sustain.synopsis.samples.client.epa.airquality;

public interface HourlyDataEntryHandler {
    void onHourlyDataEntry(HourlyDataEntry entry);
}

class MyHourlyDataEntryHandler implements HourlyDataEntryHandler {

    int count = 0;
    @Override
    public void onHourlyDataEntry(HourlyDataEntry entry) {
        count++;
    }
}