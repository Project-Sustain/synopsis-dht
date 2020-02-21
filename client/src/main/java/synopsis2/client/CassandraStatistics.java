package synopsis2.client;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import sustain.synopsis.sketch.stat.RunningStatisticsND;

@UDT(keyspace="synopsis_cassandra", name="statistics")
public class CassandraStatistics {

    @Field(name="n")
    private long n;

    @Field(name="mean")
    private double[] mean;

    @Field(name="m2")
    private double[] m2;

    @Field(name="min")
    private double[] min;

    @Field(name="max")
    private double[] max;

    @Field(name="ss")
    private double[] ss;

    // TODO fix
    public static CassandraStatistics fromRunningStatisticsND(RunningStatisticsND statNd) {
        CassandraStatistics stat = new CassandraStatistics();
        stat.n = statNd.count();
        stat.mean = statNd.means();
        stat.m2 = new double[stat.mean.length];
        stat.min = statNd.mins();
        stat.max = statNd.maxes();
        stat.ss = new double[stat.mean.length];
        return stat;
    }

    public long getN() {
        return n;
    }

    public void setN(long n) {
        this.n = n;
    }

    public double[] getMean() {
        return mean;
    }

    public void setMean(double[] mean) {
        this.mean = mean;
    }

    public double[] getM2() {
        return m2;
    }

    public void setM2(double[] m2) {
        this.m2 = m2;
    }

    public double[] getMin() {
        return min;
    }

    public void setMin(double[] min) {
        this.min = min;
    }

    public double[] getMax() {
        return max;
    }

    public void setMax(double[] max) {
        this.max = max;
    }

    public double[] getSs() {
        return ss;
    }

    public void setSs(double[] ss) {
        this.ss = ss;
    }

}
