package sustain.synopsis.samples.client.common;

public class Timer {

    boolean started = false;
    long beginNanoTime = 0;
    long endNanoTime = 0;

    public void start() {
        started = true;
        beginNanoTime = System.nanoTime();
    }

    public long stop() {
        endNanoTime = System.nanoTime();
        return endNanoTime- beginNanoTime;
    }

    public long millis() {
        return (endNanoTime-beginNanoTime) / 1000000;
    }

    public long nanos() {
        return endNanoTime-beginNanoTime;
    }

}
