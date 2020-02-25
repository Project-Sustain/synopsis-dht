package sustain.synopsis.dht.dispersion;

import org.apache.log4j.Logger;
import sustain.synopsis.dht.ServerConstants;

/**
 * @author Thilina Buddhika
 */
public class DataDispersionSchemeFactory {

    private final static DataDispersionSchemeFactory instance = new DataDispersionSchemeFactory();
    private final Logger logger = Logger.getLogger(DataDispersionSchemeFactory.class);

    private DataDispersionSchemeFactory() {
    }

    public static DataDispersionSchemeFactory getInstance() {
        return instance;
    }

    public RingIdMapper getDataDispersionScheme(String dataDispersionScheme) {
        switch (dataDispersionScheme) {
            case ServerConstants.DATA_DISPERSION_SCHEME.CONSISTENT_HASHING:
                return new ConsistentHashingDDS();
            default:
                String errMsg = "Unsupported dispersion scheme: " + dataDispersionScheme;
                logger.error(errMsg);
                throw new IllegalArgumentException(errMsg);
        }
    }
}
