package synopsis2.client;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import sustain.synopsis.common.Strand;
import sustain.synopsis.ingestion.client.core.StrandPublisher;

import java.util.*;

public class CassandraStrandPublisher implements StrandPublisher {

    private CassandraConnection connection;
    private CassandraIngestionConfig config;

    public CassandraStrandPublisher(CassandraConnection connection, CassandraIngestionConfig config) {
        this.connection = connection;
        this.config = config;
    }

    @Override
    public void publish(Set<Strand> strands) {
        BatchStatement bs = new BatchStatement();
        for (Strand s : strands) {
            CassandraStrand cs = CassandraStrand.fromStrandWithConfig(s, config);
            bs.add(getInsertStatement(cs));
        }

    }

    private Statement getInsertStatement(CassandraStrand cs) {
//        PreparedStatement preparedStatement = PreparedSt
//        Statement stmt
//

        return null;
    }

    // strandKey



}









