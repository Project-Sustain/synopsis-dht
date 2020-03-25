package sustain.synopsis.storage.lsmtree;

import org.apache.log4j.Logger;
import sustain.synopsis.storage.lsmtree.compress.BlockCompressor;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class SSTableWriter<K extends Comparable<K> & StreamSerializable, V extends StreamSerializable> {

    private final Logger logger = Logger.getLogger(SSTableWriter.class);
    private List<TableIterator<K, V>> iterators;
    private long blockSize;

    public SSTableWriter(long blockSize, List<TableIterator<K, V>> iterators) {
        this.blockSize = blockSize;
        this.iterators = iterators;
    }

    public void serialize(DataOutputStream blockOutputStream, Metadata<K> metadata, BlockCompressor compressor,
                          ChecksumGenerator checksumGenerator) throws IOException {
        SortedMergeIterator<K, V> mergeIterator = new SortedMergeIterator<>(iterators);
        ByteArrayOutputStream baos = new ByteArrayOutputStream((int) blockSize);
        DataOutputStream dos = new DataOutputStream(baos);
        K minKey = null;
        K maxKey = null;
        int blockCount = 0;
        K firstKey = null; // first key of the current block

        long uncompressedDataSize = 0;
        long compressedDataSize = 0;

        while (mergeIterator.hasNext()) {
            TableIterator.TableEntry<K, V> entry = mergeIterator.next();
            // update the block index
            if (dos.size() == 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Updated block index. Block Id: " + blockCount + ", Offset: " + blockOutputStream.size());
                }
                metadata.addBlockIndex(entry.getKey(), blockOutputStream.size());
                firstKey = entry.getKey();
            }
            // limits of the SSTable
            if (minKey == null) {
                minKey = entry.getKey();
            }
            maxKey = entry.getKey();

            entry.getKey().serialize(dos);
            entry.getValue().serialize(dos);
            if (dos.size() >= blockSize) {
                // it is possible to exceed the block size because we are not splitting an entry between two blocks
                // current block is full
                dos.flush();
                baos.flush();
                byte[] currentBlock = baos.toByteArray();
                blockOutputStream.writeBoolean(mergeIterator.hasNext()); // there is a block after the current block

                // compress and calculate checksum
                byte[] compressed = compressAndSerialize(blockOutputStream, compressor, currentBlock);
                calculateChecksum(firstKey, compressed, metadata, checksumGenerator);
                uncompressedDataSize += currentBlock.length;
                compressedDataSize += compressed.length;

                baos.reset();
                dos.close();
                dos = new DataOutputStream(baos);
                if (logger.isDebugEnabled()) {
                    logger.debug("Stored block " + blockCount + " on disk. Block size: " + currentBlock.length);
                }
                blockCount++;
            }
        }
        // last block
        if (dos.size() > 0) {
            dos.flush();
            baos.flush();
            byte[] lastBlock = baos.toByteArray();
            blockOutputStream.writeBoolean(false);
            byte[] compressed = compressAndSerialize(blockOutputStream, compressor, lastBlock);
            calculateChecksum(firstKey, compressed, metadata, checksumGenerator);
            uncompressedDataSize += lastBlock.length;
            compressedDataSize += compressed.length;

            dos.close();
            baos.close();
            if (logger.isDebugEnabled()) {
                logger.debug("Stored the final block of the SSTable on disk. Block Id: " + blockCount + ", block " +
                        "size: " + lastBlock.length);
            }
        } else if (blockCount == 0) { // empty SSTable
            blockOutputStream.writeInt(0);
            blockOutputStream.writeBoolean(false); // compressions status
            blockOutputStream.writeBoolean(false); // more blocks remaining
            if (logger.isDebugEnabled()) {
                logger.debug("Stored empty SSTable on disk.");
            }
        }

        // set the metadata
        metadata.setMin(minKey);
        metadata.setMax(maxKey);

        if (logger.isDebugEnabled()) {
            logger.debug("Completed moving a MemTable to SSTable on disk. Uncompressed size: " + uncompressedDataSize + ", compressed size: " + compressedDataSize);
        }
    }

    private byte[] compressAndSerialize(DataOutputStream blockOutputStream, BlockCompressor compressor,
                                        byte[] currentBlock) throws IOException {
        if (compressor != null) {
            byte[] compressed = compressor.compress(currentBlock);
            blockOutputStream.writeBoolean(true);
            // uncompressed length is needed for buffer allocation during deserialization
            blockOutputStream.writeInt(currentBlock.length);
            blockOutputStream.writeInt(compressed.length);
            blockOutputStream.write(compressed);
            return compressed;
        } else {
            blockOutputStream.writeBoolean(false);
            blockOutputStream.writeInt(currentBlock.length);
            blockOutputStream.write(currentBlock);
            return currentBlock;
        }
    }

    private void calculateChecksum(K firstKey, byte[] block, Metadata<K> metadata,
                                   ChecksumGenerator checksumGenerator) {
        if (checksumGenerator != null) {
            byte[] checksum = checksumGenerator.calculateChecksum(block);
            metadata.addChecksum(firstKey, checksum);
        }
    }
}
