package sustain.synopsis.dht.store.query;

import java.util.Objects;

public class Interval {
    private final long from;
    private final long to;

    public Interval(long from, long to) {
        this.from = from;
        this.to = to;
    }

    public long getFrom() {
        return from;
    }

    public long getTo() {
        return to;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Interval)) return false;
        Interval interval = (Interval) o;
        return getFrom() == interval.getFrom() && getTo() == interval.getTo();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getFrom(), getTo());
    }

    public boolean isOverlapping(Interval other) {
        if (this.from > other.from) {
            return other.to > this.from;
        }
        if (other.from > this.from) {
            return this.to > other.from;
        }
        return true; // this.from == other.from;
    }
}

