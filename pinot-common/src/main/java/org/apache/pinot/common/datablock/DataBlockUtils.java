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
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongConsumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.segment.spi.memory.CompoundDataBuffer;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.segment.spi.memory.PinotByteBuffer;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;


public final class DataBlockUtils {
  /**
   * This map is used to associate a {@link DataBlockSerde.Version} with a specific {@link DataBlockSerde}.
   *
   * Although Pinot 1.2.0 only supports a single version of the serialization format, it is possible to support
   * multiple versions in the future.
   *
   * We can imagine future version of Pinot that uses Apache Arrow to represent the data blocks.
   * In this case, in order to communicate this future version with a node running Pinot 1.2.0 we would need to
   * implement a new {@link DataBlockSerde} that can serialize and deserialize the Arrow data blocks into the
   * serialization format used by Pinot 1.2.0.
   *
   * Although this DataBlockSerde could be used to communicate two Arrow based Pinot nodes,
   * it would probably not be very efficient because in order to apply the conversion it would probably need to
   * allocate a linear amount of memory.
   * Therefore it would be recommended to have another {@link DataBlockSerde.Version} that can only be used by nodes
   * running the new version of Pinot and a new DataBlockSerde that can serialize and deserialize the Arrow data blocks
   * directly.
   *
   * Therefore that future Pinot version would have two elements in its map:
   * - One for the version used to communicate with Pinot 1.2.0
   * - Another for the version used to communicate with other nodes running the same version.
   *
   * The system right now is pretty naive and although it supports different versions at deserialization time, it
   * always uses the same to serialize the data blocks. In the future, we could add a way to specify the version
   * to use when serializing the data blocks, which could be negotiated between the two nodes or (given that the
   * clusters are not expected to run with different versions for a long time) hardcoded in the configuration.
   *
   * Anyway, having this map is very useful to profile, test and in general support different {@link DataBlockSerde},
   * even for the same format.
   */
  private static final EnumMap<DataBlockSerde.Version, DataBlockSerde> SERDES;
  private static final Pattern CAUSE_CAPTION_REGEXP = Pattern.compile("^([\\t]*)Caused by: ");
  private static final Pattern SUPPRESSED_CAPTION_REGEXP = Pattern.compile("^([\\t]*)Suppressed: ");
  private static final List<ByteString> EMPTY_BYTEBUFFER_LIST = Collections.emptyList();

  static {
    SERDES = new EnumMap<>(DataBlockSerde.Version.class);
    SERDES.put(DataBlockSerde.Version.V1_V2, new ZeroCopyDataBlockSerde());
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

  public static MetadataBlock getErrorDataBlock(Exception e) {
    if (e instanceof ProcessingException) {
      int errorCodeId = ((ProcessingException) e).getErrorCode();
      return getErrorDataBlock(Collections.singletonMap(errorCodeId, extractErrorMsg(e)));
    } else if (e instanceof QueryException) {
      int errorCodeId = ((QueryException) e).getErrorCode().getId();
      return getErrorDataBlock(Collections.singletonMap(errorCodeId, extractErrorMsg(e)));
    } else {
      // TODO: Pass in meaningful error code.
      return getErrorDataBlock(Map.of(QueryErrorCode.UNKNOWN.getId(), extractErrorMsg(e)));
    }
  }

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

  public static MetadataBlock getErrorDataBlock(Map<Integer, String> exceptions) {
    return MetadataBlock.newError(exceptions);
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
    return serialize(DataBlockSerde.Version.V1_V2, dataBlock);
  }

  @VisibleForTesting
  public static List<ByteBuffer> serialize(DataBlockSerde.Version version, DataBlock dataBlock)
      throws IOException {

    DataBlockSerde dataBlockSerde = SERDES.get(version);
    if (dataBlockSerde == null) {
      throw new UnsupportedOperationException("Unsupported data block version: " + version);
    }

    DataBlock.Type dataBlockType = dataBlock.getDataBlockType();
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

  // Returns a list of ByteString instances hosting the bytes obtained from the input block serialization. Returned
  // instances will not exceed the maximum size unless one single byte buffer from the serialization exceeds it.
  public static List<ByteString> toByteStrings(DataBlock dataBlock, int maxByteStringSize)
      throws IOException {
    List<ByteBuffer> bytes = dataBlock.serialize();
    if (bytes.isEmpty()) {
      return EMPTY_BYTEBUFFER_LIST;
    }

    int totalBytes = 0;
    for (ByteBuffer bb : bytes) {
      totalBytes += bb.remaining();
    }
    int initialCapacity = (totalBytes / maxByteStringSize) + bytes.size();
    List<ByteString> byteStrings = new ArrayList<>(initialCapacity);

    ByteString current = UnsafeByteOperations.unsafeWrap(bytes.get(0));
    for (int i = 1; i < bytes.size(); i++) {
      ByteBuffer bb = bytes.get(i);
      if (current.size() + bb.remaining() > maxByteStringSize) {
        byteStrings.add(current);
        current = UnsafeByteOperations.unsafeWrap(bb);
      } else {
        current = current.concat(UnsafeByteOperations.unsafeWrap(bb));
      }
    }
    byteStrings.add(current);
    return byteStrings;
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
    List<DataBuffer> dataBuffers = buffers.stream()
        .map(PinotByteBuffer::wrap)
        .collect(Collectors.toList());
    try (CompoundDataBuffer compoundBuffer = new CompoundDataBuffer(dataBuffers, ByteOrder.BIG_ENDIAN, false)) {
      return deserialize(compoundBuffer);
    }
  }

  /**
   * Deserialize a list of byte buffers into a data block.
   * Contrary to {@link #readFrom(ByteBuffer)}, the given buffers will not be modified.
   */
  public static DataBlock deserialize(ByteBuffer[] buffers)
      throws IOException {
    try (CompoundDataBuffer compoundBuffer = new CompoundDataBuffer(buffers, ByteOrder.BIG_ENDIAN, false)) {
      return deserialize(compoundBuffer);
    }
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
    int versionAndSubVersion = buffer.getInt(offset);
    int version = getVersion(versionAndSubVersion);
    DataBlockSerde dataBlockSerde;
    try {
      dataBlockSerde = SERDES.get(DataBlockSerde.Version.fromInt(version));
    } catch (Exception e) {
      throw new IOException("Failed to get serde for version: " + version, e);
    }

    DataBlock.Type type;
    try {
      type = getType(versionAndSubVersion);
    } catch (Exception e) {
      throw new IOException("Failed to get type for version: " + version, e);
    }

    try {
      return dataBlockSerde.deserialize(buffer, 0, type, finalOffsetConsumer);
    } catch (Exception e) {
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
