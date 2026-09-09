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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.function.LongConsumer;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.segment.spi.memory.CompoundDataBuffer;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.segment.spi.memory.PinotByteBuffer;


public final class DataBlockUtils {
  /**
   * This map is used to associate a {@link DataBlockSerde.Version} with a specific {@link DataBlockSerde}.
   * Arrow blocks use IPC; existing row, columnar and metadata blocks retain their legacy wire format.
   */
  private static final EnumMap<DataBlockSerde.Version, DataBlockSerde> SERDES;
  private static final Pattern CAUSE_CAPTION_REGEXP = Pattern.compile("^([\\t]*)Caused by: ");
  private static final Pattern SUPPRESSED_CAPTION_REGEXP = Pattern.compile("^([\\t]*)Suppressed: ");

  static {
    SERDES = new EnumMap<>(DataBlockSerde.Version.class);
    SERDES.put(DataBlockSerde.Version.V1_V2, new ZeroCopyDataBlockSerde());
    SERDES.put(DataBlockSerde.Version.ARROW_IPC, new ArrowDataBlockSerde());
  }

  @VisibleForTesting
  public static DataBlockSerde getSerde(DataBlockSerde.Version version) {
    return SERDES.get(version);
  }

  @VisibleForTesting
  public static void setSerde(DataBlockSerde.Version version, DataBlockSerde serde) {
    SERDES.put(version, serde);
  }

  private DataBlockUtils() {
  }

  static final int VERSION_TYPE_SHIFT = 5;

  public static String extractErrorMsg(Throwable t) {
    while (t.getCause() != null && t.getMessage() == null) {
      t = t.getCause();
    }
    return t.getMessage() + "\n" + getTruncatedStackTrace(t);
  }

  /**
   * Truncate the stack trace of the given {@link Throwable} to a maximum of 5 lines per frame.
   * <p>
   * This method is deprecated because it is not used in the codebase and it is not clear what is the purpose of
   * truncating the stack trace.
   * <p>
   * The method is kept here for reference and in case it is needed in the future.
   *
   * @deprecated We still need to think whether and how to send stack traces downstream
   */
  @Deprecated
  private static String getTruncatedStackTrace(Throwable t) {
    StringWriter stringWriter = new StringWriter();
    t.printStackTrace(new PrintWriter(stringWriter));
    String fullStackTrace = stringWriter.toString();
    String[] lines = StringUtils.split(fullStackTrace, '\n');
    // exception should at least have one line, no need to check here.
    StringBuilder sb = new StringBuilder(lines[0]);
    int lineOfStackTracePerFrame = 1;
    int maxLinesOfStackTracePerFrame = 5;
    for (int i = 1; i < lines.length; i++) {
      if (CAUSE_CAPTION_REGEXP.matcher(lines[i]).find() || SUPPRESSED_CAPTION_REGEXP.matcher(lines[i]).find()) {
        // reset stack trace print counter when a new cause or suppressed Throwable were found.
        if (lineOfStackTracePerFrame >= maxLinesOfStackTracePerFrame) {
          sb.append('\n').append("...");
        }
        sb.append('\n').append(lines[i]);
        lineOfStackTracePerFrame = 1;
      } else if (lineOfStackTracePerFrame < maxLinesOfStackTracePerFrame) {
        // only print numLinesOfStackTrace stack trace and ignore any additional lines.
        sb.append('\n').append(lines[i]);
        lineOfStackTracePerFrame++;
      }
    }
    return sb.toString();
  }

  /**
   * Reads an integer from the given byte buffer.
   * <p>
   * The returned integer contains both the version and the type of the data block.
   * {@link #getVersion(int)} and {@link #getType(int)} can be used to extract the version and the type.
   * @param byteBuffer byte buffer to read from. A single int will be read
   */
  public static int readVersionType(ByteBuffer byteBuffer) {
    return byteBuffer.getInt();
  }

  public static int getVersion(int versionType) {
    return versionType & ((1 << VERSION_TYPE_SHIFT) - 1);
  }

  public static DataBlock.Type getType(int versionType) {
    return DataBlock.Type.fromOrdinal(versionType >> VERSION_TYPE_SHIFT);
  }

  public static List<ByteBuffer> serialize(DataBlock dataBlock)
      throws IOException {
    return serialize(dataBlock instanceof ArrowDataBlock ? DataBlockSerde.Version.ARROW_IPC
        : DataBlockSerde.Version.V1_V2, dataBlock);
  }

  @VisibleForTesting
  public static List<ByteBuffer> serialize(DataBlockSerde.Version version, DataBlock dataBlock)
      throws IOException {

    DataBlockSerde dataBlockSerde = SERDES.get(version);
    if (dataBlockSerde == null) {
      throw new UnsupportedOperationException("Unsupported data block version: " + version);
    }

    DataBlock.Type dataBlockType = dataBlock.getDataBlockType();
    if ((version == DataBlockSerde.Version.ARROW_IPC) != (dataBlockType == DataBlock.Type.ARROW)) {
      throw new IOException("Incompatible data block type " + dataBlockType + " for version " + version);
    }
    int firstInt = version.getVersion() + (dataBlockType.ordinal() << DataBlockUtils.VERSION_TYPE_SHIFT);

    DataBuffer dataBuffer = dataBlockSerde.serialize(dataBlock, firstInt);

    int readFirstByte;
    if (dataBuffer.order() != ByteOrder.BIG_ENDIAN) {
      readFirstByte = dataBuffer.view(0, 4, ByteOrder.BIG_ENDIAN).getInt(0);
    } else {
      readFirstByte = dataBuffer.getInt(0);
    }
    Preconditions.checkState(readFirstByte == firstInt, "Illegal serialization by {}. "
        + "The first integer should be {} but is {} instead", dataBuffer.getClass().getName(), firstInt, readFirstByte);

    ArrayList<ByteBuffer> result = new ArrayList<>();
    dataBuffer.appendAsByteBuffers(result);
    return result;
  }

  public static ByteString toByteString(DataBlock dataBlock)
      throws IOException {
    List<ByteBuffer> bytes = dataBlock.serialize();
    ByteString byteString;
    if (bytes.isEmpty()) {
      byteString = ByteString.EMPTY;
    } else {
      byteString = UnsafeByteOperations.unsafeWrap(bytes.get(0));
      for (int i = 1; i < bytes.size(); i++) {
        byteString = byteString.concat(UnsafeByteOperations.unsafeWrap(bytes.get(i)));
      }
    }
    return byteString;
  }

  /**
   * Reads a data block from the given byte buffer.
   * @param buffer the buffer to read from. The data will be read at the buffer's current position. This position will
   *               be updated to point to the end of the data block.
   */
  public static DataBlock readFrom(ByteBuffer buffer)
      throws IOException {
    return deserialize(PinotByteBuffer.wrap(buffer), buffer.position(), newOffset -> {
      if (newOffset > Integer.MAX_VALUE) {
        throw new IllegalStateException("Data block is too large");
      }
      buffer.position((int) newOffset);
    });
  }

  /**
   * Deserialize a list of byte buffers into a data block.
   * Contrary to {@link #readFrom(ByteBuffer)}, the given buffers will not be modified.
   */
  public static DataBlock deserialize(List<ByteBuffer> buffers)
      throws IOException {
    DataBuffer dataBuffer = buffers.size() == 1 ? PinotByteBuffer.wrap(buffers.get(0))
        : CompoundDataBuffer.fromByteBuffers(buffers, ByteOrder.BIG_ENDIAN, false);
    return deserialize(dataBuffer);
  }

  /**
   * Deserializes without modifying the input buffers. Returned Arrow blocks own memory in {@code allocator}
   * and must be closed by the caller.
   */
  public static DataBlock deserialize(List<ByteBuffer> buffers, BufferAllocator allocator)
      throws IOException {
    if (buffers.size() == 1) {
      return deserialize(PinotByteBuffer.slice(buffers.get(0)), 0, null, allocator);
    }
    CompoundDataBuffer.Builder builder = new CompoundDataBuffer.Builder(ByteOrder.BIG_ENDIAN, false);
    for (ByteBuffer buffer : buffers) {
      builder.addBuffer(PinotByteBuffer.slice(buffer));
    }
    return deserialize(builder.build(), 0, null, allocator);
  }

  /**
   * Deserialize a list of byte buffers into a data block.
   * Contrary to {@link #readFrom(ByteBuffer)}, the given buffers will not be modified.
   */
  public static DataBlock deserialize(ByteBuffer[] buffers)
      throws IOException {
    DataBuffer dataBuffer = buffers.length == 1 ? PinotByteBuffer.wrap(buffers[0])
        : CompoundDataBuffer.fromByteBuffers(buffers, ByteOrder.BIG_ENDIAN, false);
    return deserialize(dataBuffer);
  }

  /**
   * Deserialize a list of byte buffers into a data block.
   * <p>
   * Data will be read from the first byte of the buffer. Use {@link #deserialize(DataBuffer, long, LongConsumer)}
   * in case it is needed to read from a different position.
   */
  public static DataBlock deserialize(DataBuffer buffer)
      throws IOException {
    return deserialize(buffer, 0, null);
  }

  /**
   * Deserialize a list of byte buffers into a data block.
   * @param buffer the buffer to read from.
   * @param offset the offset in the buffer where the data starts.
   * @param finalOffsetConsumer An optional consumer that will be called after the data block is deserialized.
   *                            The consumer will receive the offset where the data block ends.
   */
  public static DataBlock deserialize(DataBuffer buffer, long offset, @Nullable LongConsumer finalOffsetConsumer)
      throws IOException {
    return deserialize(buffer, offset, finalOffsetConsumer, null);
  }

  /**
   * Reads one block at {@code offset}, reporting its absolute end offset on success. The caller owns both the supplied
   * allocator and any returned Arrow block; this method neither creates nor closes a process-level allocator.
   */
  public static DataBlock deserialize(DataBuffer buffer, long offset, @Nullable LongConsumer finalOffsetConsumer,
      @Nullable BufferAllocator allocator)
      throws IOException {
    if (offset < 0 || offset > buffer.size() - Integer.BYTES) {
      throw new IOException("Truncated data block header at offset: " + offset);
    }
    int versionAndSubVersion = buffer.view(offset, offset + Integer.BYTES, ByteOrder.BIG_ENDIAN).getInt(0);
    int version = getVersion(versionAndSubVersion);
    DataBlockSerde dataBlockSerde;
    try {
      dataBlockSerde = SERDES.get(DataBlockSerde.Version.fromInt(version));
    } catch (IllegalArgumentException e) {
      throw new IOException("Failed to get serde for version: " + version, e);
    }

    DataBlock.Type type;
    try {
      type = getType(versionAndSubVersion);
    } catch (IllegalArgumentException e) {
      throw new IOException("Failed to get type for version: " + version, e);
    }

    if ((version == DataBlockSerde.Version.ARROW_IPC.getVersion()) != (type == DataBlock.Type.ARROW)) {
      throw new IOException("Incompatible data block type " + type + " for version " + version);
    }
    if (type == DataBlock.Type.ARROW && allocator == null) {
      throw new IOException("Arrow IPC deserialization requires a caller-owned BufferAllocator");
    }

    try {
      if (type != DataBlock.Type.ARROW) {
        // Legacy headers contain block-relative offsets, including offsets used for metadata and finalOffset.
        DataBuffer blockBuffer = buffer.view(offset, buffer.size(), ByteOrder.BIG_ENDIAN);
        return dataBlockSerde.deserialize(blockBuffer, 0, type,
            finalOffsetConsumer == null ? null : end -> finalOffsetConsumer.accept(offset + end));
      }
      return dataBlockSerde.deserialize(buffer, offset, type, finalOffsetConsumer, allocator);
    } catch (IOException | RuntimeException e) {
      throw new IOException("Failed to deserialize data block with serde " + dataBlockSerde.getClass(), e);
    }
  }

  /**
   * Given a {@link DataSchema}, compute each column's offset and fill them into the passed in array, then return the
   * row size in bytes.
   *
   * @param dataSchema data schema.
   * @param columnOffsets array of column offsets.
   * @return row size in bytes.
   */
  public static int computeColumnOffsets(DataSchema dataSchema, int[] columnOffsets) {
    int numColumns = columnOffsets.length;
    assert numColumns == dataSchema.size();

    ColumnDataType[] storedColumnDataTypes = dataSchema.getStoredColumnDataTypes();
    int rowSizeInBytes = 0;
    for (int i = 0; i < numColumns; i++) {
      columnOffsets[i] = rowSizeInBytes;
      switch (storedColumnDataTypes[i]) {
        case INT:
          rowSizeInBytes += 4;
          break;
        case LONG:
          rowSizeInBytes += 8;
          break;
        case FLOAT:
          rowSizeInBytes += 4;
          break;
        case DOUBLE:
          rowSizeInBytes += 8;
          break;
        case STRING:
          rowSizeInBytes += 4;
          break;
        // Object and array. (POSITION|LENGTH)
        default:
          rowSizeInBytes += 8;
          break;
      }
    }

    return rowSizeInBytes;
  }

  /**
   * Given a {@link DataSchema}, compute each column's size and fill them into the passed in array.
   *
   * @param dataSchema data schema.
   * @param columnSizes array of column size.
   * @return row size in bytes.
   */
  public static void computeColumnSizeInBytes(DataSchema dataSchema, int[] columnSizes) {
    int numColumns = columnSizes.length;
    assert numColumns == dataSchema.size();

    ColumnDataType[] storedColumnDataTypes = dataSchema.getStoredColumnDataTypes();
    for (int i = 0; i < numColumns; i++) {
      switch (storedColumnDataTypes[i]) {
        case INT:
          columnSizes[i] = 4;
          break;
        case LONG:
          columnSizes[i] = 8;
          break;
        case FLOAT:
          columnSizes[i] = 4;
          break;
        case DOUBLE:
          columnSizes[i] = 8;
          break;
        case STRING:
          columnSizes[i] = 4;
          break;
        // Object and array. (POSITION|LENGTH)
        default:
          columnSizes[i] = 8;
          break;
      }
    }
  }
}
