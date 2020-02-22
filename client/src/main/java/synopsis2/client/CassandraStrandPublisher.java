package synopsis2.client;

import com.datastax.driver.mapping.Mapper;
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
        Mapper<CassandraStrand> mapper = this.connection.getManager().mapper(CassandraStrand.class);

        for (Strand s : strands) {
            CassandraStrand cs = CassandraStrand.fromStrandWithConfig(s, config);
            mapper.save(cs);
        }
    }

}









