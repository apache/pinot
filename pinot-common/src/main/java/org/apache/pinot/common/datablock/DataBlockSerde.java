package org.apache.pinot.common.datablock;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.segment.spi.memory.PinotInputStream;
import org.apache.pinot.segment.spi.memory.PinotOutputStream;


/**
 * An interface that can be implemented to support different types of data block serialization and deserialization.
 */
public interface DataBlockSerde {

  /**
   * Serialize the data block into a buffer.
   * @param dataBlock The data block to serialize.
   * @param firstInt The first integer, which is used to codify the version and type of the data block in a protocol
   *                 defined way. This integer must be written in the first 4 positions of the buffer in BIG_ENDIAN
   *                 order.
   */
  DataBuffer serialize(DataBlock.Raw dataBlock, int firstInt)
      throws IOException;

  /**
   * Serialize the data block into the given output stream.
   *
   * @param buffer The buffer that contains the data. It will always use {@link java.nio.ByteOrder#BIG_ENDIAN} order.
   * @param offset the offset in the buffer where the data starts. The first integer is reserved to store version and
   *               type and should not be trusted by the implementation. Use the type parameter instead.
   * @param type   the type of data block.
   */
  DataBlock deserialize(DataBuffer buffer, long offset, DataBlock.Type type)
      throws IOException;

  Version getVersion();

  /**
   * The version used by this implementation.
   * <p>
   * The version should be incremented whenever the serialization format changes in a way that is not backwards
   * compatible in both serialization and deserialization ways.
   */
  enum Version {
    V2(2);

    private final int _version;

    Version(int version) {
      _version = version;
    }

    public static Version fromInt(int version) {
      switch (version) {
        case 1:
        case 2:
          return V2;
        default:
          throw new IllegalArgumentException("Unknown version: " + version);
      }
    }
  }
}
