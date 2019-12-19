package sustain.synopsis.storage.lsmtree;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * Provides checksum validation on blocks using SHA-1.
 */
public class ChecksumGenerator {
    public class ChecksumError extends Exception {
        ChecksumError(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private final MessageDigest digest;

    public ChecksumGenerator() throws ChecksumError {
        try {
            digest = MessageDigest.getInstance("SHA1");
        } catch (NoSuchAlgorithmException e) {
            throw new ChecksumError("Error initializing ChecksumGenerator", e);
        }
    }

    /**
     * Calculate checksum
     * @param block Block on which the checksum is calculated
     * @return Calculated checksum
     */
    public byte[] calculateChecksum(byte[] block) {
        return digest.digest(block);
    }

    /**
     * Validate a given checksum
     * @param block Data on which a checksum was calculated previously
     * @param checksum  Previously calculated checksum
     * @return <code>true</code> if the checksum matches with the data
     */
    public boolean validateChecksum(byte[] block, byte[] checksum){
        return Arrays.equals(calculateChecksum(block), checksum);
    }
}
