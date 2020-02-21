package synopsis2.client;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraConnection {

    private Cluster cluster;
    private Session session;

    public CassandraConnection(String host) {
        cluster = Cluster.builder()
                .addContactPoint(host)
                .build();
        session = cluster.connect();
    }

    public void close() {
        cluster.close();
    }

    public Session getSession() {
        return session;
    }

}
