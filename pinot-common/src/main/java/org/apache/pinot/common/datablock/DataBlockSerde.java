/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.datablock;

import java.io.IOException;
import java.util.function.LongConsumer;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.memory.DataBuffer;


/**
 * An interface that can be implemented to support different types of data block serialization and deserialization.
 * <p>
 * It is important to distinguish between the serialization format and the memory layout used by the data block.
 * DataBlocks are the objects we use to communicate different Operators in the same OpChain.
 * Given these Operators will always be in the same JVM, the memory layout may be dependent on the Pinot runtime
 * version.
 * Two Pinot nodes in different versions may represent the same data using different DataBlocks but there
 * should be at least one common serialization format (defined by the {@link Version} used) that can be used to
 * serialize and deserialize the data block between the two nodes.
 *
 * For example, Pinot versions 1.0.0, 1.1.0 and 1.2.0 use slightly different memory layouts for the data blocks, but
 * they can send blocks to each other using the same serialization format (specifically, version
 * {@link DataBlockSerde.Version#V1_V2}).
 *
 * @see DataBlockUtils to see how these serdes are being used.
 */
public interface DataBlockSerde {

  /**
   * Serialize the data block into a buffer.
   * @param dataBlock the data block to serialize.
   * @param firstInt The first integer, which is used to codify the version and type of the data block in a protocol
   *                 defined way. This integer must be written in the first 4 positions of the buffer in BIG_ENDIAN
   *                 order.
   */
  DataBuffer serialize(DataBlock dataBlock, int firstInt)
      throws IOException;

  /**
   * Deserialize the data block into a DataBlock.
   *
   * @param buffer the buffer that contains the data.
   * @param offset the offset in the buffer where the data starts. The first integer is reserved to store version and
   *               type and should not be trusted by the implementation. Use the type parameter instead.
   * @param type   the type of data block.
   * @param finalOffsetConsumer A consumer that will be called after the data block is deserialized. The consumer will
   *                            receive the offset where the data block ends.
   */
  DataBlock deserialize(DataBuffer buffer, long offset, DataBlock.Type type, @Nullable LongConsumer finalOffsetConsumer)
      throws IOException;

  default DataBlock deserialize(DataBuffer buffer, long offset, DataBlock.Type type)
      throws IOException {
    return deserialize(buffer, offset, type, null);
  }

  Version getVersion();

  /**
   * The version used by this implementation.
   * <p>
   * The version should be incremented whenever the serialization format changes in a way that is not backwards
   * compatible in both serialization and deserialization ways.
   */
  enum Version {
    /**
     * The version used Pinot 1.0, 1.1 and 1.2.
     * <p>
     * Older Pinot versions use id 1 to identify their version.
     */
    V1_V2(2);

    private final int _version;

    Version(int version) {
      _version = version;
    }

    public static Version fromInt(int version) {
      switch (version) {
        case 1:
        case 2:
          return V1_V2;
        default:
          throw new IllegalArgumentException("Unknown version: " + version);
      }
    }

    public int getVersion() {
      return _version;
    }
  }
}
