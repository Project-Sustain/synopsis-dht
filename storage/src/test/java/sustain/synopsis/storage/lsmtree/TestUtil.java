package sustain.synopsis.storage.lsmtree;

import java.util.Random;

class TestUtil {

    /**
     * Generate a {@link TableIterator} with keys start, start + 1 * step size, ... ,
     * start + (count - 1) * step size. {@link LSMTestValue} is generated such that the total entry size
     * is equal to the given entry size
     * @param start Start value of the key sequence
     * @param stepSize Step size between two adjacent keys
     * @param count Number of entries to generate
     * @param entrySize Total entry size including both the key and the value
     * @param random Seed to used to generate the random bytes for the payload
     * @return {@link TableIterator} instance
     */
    static TableIterator<LSMTestKey, LSMTestValue> getIterator(int start, int stepSize, int count, int entrySize,
                                                                Random random) {
        return new TableIterator<LSMTestKey, LSMTestValue>() {
            int i = 0;

            @Override
            public boolean hasNext() {
                return i < count;
            }

            @Override
            public TableEntry<LSMTestKey, LSMTestValue> next() {
                byte[] payload = new byte[entrySize - 8]; // key is 4 byes, length encoding takes 4 bytes
                random.nextBytes(payload);
                return new TableEntry<>(new LSMTestKey(start + i++ * stepSize), new LSMTestValue(payload));
            }
        };
    }
}
