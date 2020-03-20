package sustain.synopsis.storage.lsmtree;

import org.apache.log4j.Logger;
import sustain.synopsis.storage.lsmtree.compress.BlockCompressor;
import sustain.synopsis.storage.lsmtree.compress.LZ4BlockCompressor;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.Iterator;

import static java.nio.file.StandardOpenOption.READ;
import static sustain.synopsis.storage.lsmtree.Util.readFully;

/**
 * @param <K>
 */
public class SSTableReader<K extends Comparable<K> & StreamSerializable> {

    private final Logger logger = Logger.getLogger(SSTableReader.class);
    private final Metadata<K> metadata;
    private final Class<K> clazz;
    private final BlockCompressor compressor = new LZ4BlockCompressor();

    public SSTableReader(Metadata<K> metadata, Class<K> kClazz) {
        this.metadata = metadata;
        this.clazz = kClazz;
    }

    /**
     * Returns an iterator of key and serialized value pairs
     *
     * @param key corresponding to the the offset - the block starts with the entry with this key
     * @return An {@link Iterator<sustain.synopsis.storage.lsmtree.TableIterator.TableEntry>} of the block data
     * @throws IOException Error reading from disk
     */
    public Iterator<TableIterator.TableEntry<K, byte[]>> readBlock(K key) throws IOException {
        Integer offset = metadata.getBlockIndex().get(key);
        if (offset == null) {
            throw new IOException("Invalid offset: " + offset + ", available range: " + metadata.getBlockIndex().firstEntry().getValue() + ", " + metadata.getBlockIndex().lastEntry().getValue());
        }
        final SeekableByteChannel channel =
                FileChannel.open(new File(metadata.getPath()).toPath(), READ).position(offset);
        byte[] data = extractBlockData(channel);
        return getPairIterator(data);
    }

    Iterator<TableIterator.TableEntry<K, byte[]>> getPairIterator(byte[] data) {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);
        return new Iterator<TableIterator.TableEntry<K, byte[]>>() {
            @Override
            public boolean hasNext() {
                return bais.available() > 0;
            }

            @Override
            public TableIterator.TableEntry<K, byte[]> next() {
                try {
                    K key = clazz.newInstance();
                    key.deserialize(dis);
                    byte[] value = new byte[dis.readInt()];
                    dis.readFully(value);
                    return new TableIterator.TableEntry<>(key, value);
                } catch (InstantiationException | IllegalAccessException e) {
                    logger.error("Reflection error when creating instace of " + clazz.getName(), e);
                } catch (IOException e) {
                    logger.error("Error reading from the block.", e);
                }
                return null;
            }
        };
    }

    byte[] extractBlockData(SeekableByteChannel channel) throws IOException {
        // read all metadata in once (reduces the number of Byte Buffer instantiations)
        /* metadata size = last block (bool - 1 Byte) + compressed (bool - 1 Byte) + uncompressed_length (int - 4 Bytes)
                            + compressed_length (int - 4 bytes, optional)*/
        ByteBuffer metadataBuffer = ByteBuffer.allocate(10);
        readFully(channel, metadataBuffer);
        metadataBuffer.get(); // we do not use the isLastBlock flag here - read and discard
        boolean isCompressed = metadataBuffer.get() != 0;
        int uncompressedLength = metadataBuffer.getInt();
        int dataBlockLength;
        int compressedLength;
        if (isCompressed) {
            compressedLength = metadataBuffer.getInt();
            dataBlockLength = compressedLength;
        } else {
            dataBlockLength = uncompressedLength;
            // we have read 4 bytes from the data block
            channel.position(channel.position() - 4);
        }
        // read the data block
        ByteBuffer buffer = ByteBuffer.allocate(dataBlockLength);
        readFully(channel, buffer);
        return isCompressed ? compressor.decompress(uncompressedLength, buffer.array()) : buffer.array();
    }
}
