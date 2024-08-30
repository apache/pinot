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
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongConsumer;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.segment.spi.memory.CompoundDataBuffer;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.segment.spi.memory.PagedPinotOutputStream;
import org.apache.pinot.segment.spi.memory.PinotByteBuffer;
import org.apache.pinot.segment.spi.memory.PinotInputStream;
import org.apache.pinot.segment.spi.memory.PinotOutputStream;


/**
 * An efficient serde that implements {@link DataBlockSerde.Version#V1_V2} using trying to make as fewer copies as
 * possible.
 */
public class ZeroCopyDataBlockSerde implements DataBlockSerde {

  private final PagedPinotOutputStream.PageAllocator _allocator;

  public ZeroCopyDataBlockSerde() {
    _allocator = PagedPinotOutputStream.HeapPageAllocator.createSmall();
  }

  public ZeroCopyDataBlockSerde(PagedPinotOutputStream.PageAllocator allocator) {
    _allocator = allocator;
  }

  @Override
  public DataBuffer serialize(DataBlock dataBlock, int firstInt)
      throws IOException {
    Header header = new Header(firstInt, dataBlock.getNumberOfRows(), dataBlock.getNumberOfColumns());

    CompoundDataBuffer.Builder builder = new CompoundDataBuffer.Builder(ByteOrder.BIG_ENDIAN, false);

    ByteBuffer headerBuffer = ByteBuffer.allocate(Header.BYTES);
    builder.addBuffer(PinotByteBuffer.wrap(headerBuffer));

    try (PagedPinotOutputStream into = new PagedPinotOutputStream(_allocator)) {
      serializeExceptions(dataBlock, into);
      header._exceptionsLength = (int) into.getCurrentOffset();
      serializeDictionary(dataBlock, into);
      header._dictionaryLength = (int) into.getCurrentOffset() - header._exceptionsLength;
      serializeDataSchema(dataBlock, into);
      header._dataSchemaLength = (int) into.getCurrentOffset() - header._exceptionsLength - header._dictionaryLength;

      builder.addPagedOutputStream(into);
    }

    DataBuffer fixedData = dataBlock.getFixedData();
    if (fixedData != null) {
      builder.addBuffer(fixedData);
      header._fixedSizeDataLength = (int) fixedData.size();
    }

    DataBuffer varSizeData = dataBlock.getVarSizeData();
    if (varSizeData != null) {
      builder.addBuffer(varSizeData);
      header._variableSizeDataLength = (int) varSizeData.size();
    }

    try (PagedPinotOutputStream into = new PagedPinotOutputStream(_allocator)) {
      serializeMetadata(dataBlock, into, header);
      builder.addPagedOutputStream(into);
    }

    header.updateStarts();
    header.serialize(headerBuffer);
    headerBuffer.flip();

    return builder.build();
  }

  /**
   * Serialize the exceptions map into the {@link PinotOutputStream} current position.
   * <p>
   * the header object is modified to update {@link Header#_exceptionsLength}
   */
  private void serializeExceptions(DataBlock dataBlock, PinotOutputStream into)
      throws IOException {

    Map<Integer, String> errCodeToExceptionMap = dataBlock.getExceptions();
    if (errCodeToExceptionMap == null || errCodeToExceptionMap.isEmpty()) {
      return;
    }

    into.writeInt(errCodeToExceptionMap.size());
    for (Map.Entry<Integer, String> entry : errCodeToExceptionMap.entrySet()) {
      into.writeInt(entry.getKey());
      into.writeInt4String(entry.getValue());
    }
  }

  private static void serializeDictionary(DataBlock dataBlock, PinotOutputStream into)
      throws IOException {
    String[] stringDictionary = dataBlock.getStringDictionary();
    if (stringDictionary == null) {
      // TODO: probably we can also do this when length is 0
      return;
    }

    into.writeInt(stringDictionary.length);
    for (String entry : stringDictionary) {
      into.writeInt4String(entry);
    }
  }

  private static void serializeDataSchema(DataBlock dataBlock, PinotOutputStream into)
      throws IOException {
    DataSchema dataSchema = dataBlock.getDataSchema();
    if (dataSchema == null) {
      return;
    }

    // TODO: This is actually allocating, we can improve it in the future if it is problematic
    byte[] bytes = dataSchema.toBytes();
    into.write(bytes);
  }

  private static void serializeMetadata(DataBlock dataBlock, PinotOutputStream into, Header header)
      throws IOException {
    header._metadataStart = (int) into.getCurrentOffset();
    if (!(dataBlock instanceof MetadataBlock)) {
      into.writeInt(0);
      return;
    }
    List<DataBuffer> statsByStage = dataBlock.getStatsByStage();
    if (statsByStage == null) {
      into.writeInt(0);
      return;
    }
    int size = statsByStage.size();
    into.writeInt(size);
    if (size > 0) {
      for (DataBuffer stat : statsByStage) {
        if (stat == null) {
          into.writeBoolean(false);
        } else {
          into.writeBoolean(true);
          if (stat.size() > Integer.MAX_VALUE) {
            throw new IOException("Stat size is too large to serialize");
          }
          into.writeInt((int) stat.size());
          into.write(stat);
        }
      }
    }
  }

  @Override
  public DataBlock deserialize(DataBuffer buffer, long offset, DataBlock.Type type,
      @Nullable LongConsumer finalOffsetConsumer)
      throws IOException {
    try (PinotInputStream stream = buffer.openInputStream(offset)) {
      Header header = Header.deserialize(stream);

      if (finalOffsetConsumer != null) {
        finalOffsetConsumer.accept(offset + calculateEndOffset(buffer, header));
      }

      switch (type) {
        case COLUMNAR:
          return new ColumnarDataBlock(header._numRows, deserializeDataSchema(stream, header),
              deserializeDictionary(stream, header),
              bufferView(buffer, header._fixedSizeDataStart + offset, header._fixedSizeDataLength),
              bufferView(buffer, header._variableSizeDataStart + offset, header._variableSizeDataLength));
        case ROW:
          return new RowDataBlock(header._numRows, deserializeDataSchema(stream, header),
              deserializeDictionary(stream, header),
              bufferView(buffer, header._fixedSizeDataStart + offset, header._fixedSizeDataLength),
              bufferView(buffer, header._variableSizeDataStart + offset, header._variableSizeDataLength));
        case METADATA: {
          Map<Integer, String> exceptions = deserializeExceptions(stream, header);
          if (!exceptions.isEmpty()) {
            return MetadataBlock.newError(exceptions);
          } else {
            List<DataBuffer> metadata = deserializeMetadata(buffer, header);
            return new MetadataBlock(metadata);
          }
        }
        default:
          throw new UnsupportedOperationException("Unsupported data table version: " + getVersion() + " with type: "
              + type);
      }
    }
  }

  private static List<DataBuffer> deserializeMetadata(DataBuffer buffer, Header header) {
    long currentOffset = header._metadataStart;
    int statsSize = buffer.getInt(currentOffset);
    currentOffset += Integer.BYTES;

    List<DataBuffer> stats = new ArrayList<>(statsSize);

    for (int i = 0; i < statsSize; i++) {
      boolean isPresent = buffer.getByte(currentOffset) != 0;
      currentOffset += Byte.BYTES;
      if (isPresent) {
        int length = buffer.getInt(currentOffset);
        currentOffset += Integer.BYTES;
        stats.add(buffer.view(currentOffset, currentOffset + length));
        currentOffset += length;
      } else {
        stats.add(null);
      }
    }
    return stats;
  }

  private long calculateEndOffset(DataBuffer buffer, Header header) {
    long currentOffset = header._metadataStart;
    int statsSize = buffer.getInt(currentOffset);
    currentOffset += Integer.BYTES;

    for (int i = 0; i < statsSize; i++) {
      boolean isPresent = buffer.getByte(currentOffset) != 0;
      currentOffset += Byte.BYTES;
      if (isPresent) {
        int length = buffer.getInt(currentOffset);
        currentOffset += Integer.BYTES;
        currentOffset += length;
      }
    }
    return currentOffset;
  }

  @VisibleForTesting
  static Map<Integer, String> deserializeExceptions(PinotInputStream stream, Header header)
      throws IOException {
    if (header._exceptionsLength == 0) {
      return new HashMap<>();
    }
    stream.seek(header.getExceptionsStart());
    int numExceptions = stream.readInt();
    Map<Integer, String> exceptions = new HashMap<>(HashUtil.getHashMapCapacity(numExceptions));
    for (int i = 0; i < numExceptions; i++) {
      int errCode = stream.readInt();
      String errMessage = stream.readInt4UTF();
      exceptions.put(errCode, errMessage);
    }
    return exceptions;
  }

  private DataBuffer bufferView(DataBuffer buffer, long offset, int length) {
    if (length == 0) {
      return PinotByteBuffer.empty();
    }
    return buffer.view(offset, offset + length, ByteOrder.BIG_ENDIAN);
  }

  private static String[] deserializeDictionary(PinotInputStream stream, Header header)
      throws IOException {
    if (header._dictionaryLength == 0) {
      return new String[0];
    }
    stream.seek(header._dictionaryStart);

    int dictionarySize = stream.readInt();
    String[] stringDictionary = new String[dictionarySize];
    for (int i = 0; i < dictionarySize; i++) {
      stringDictionary[i] = stream.readInt4UTF();
    }
    return stringDictionary;
  }

  private static DataSchema deserializeDataSchema(PinotInputStream stream, Header header)
      throws IOException {
    if (header._dataSchemaLength == 0) {
      return null;
    }
    stream.seek(header._dataSchemaStart);
    return DataSchema.fromBytes(stream);
  }

  @Override
  public Version getVersion() {
    return Version.V1_V2;
  }

  static class Header {
    static final int BYTES = 13 * Integer.BYTES;

    // first int is used for versioning
    int _firstInt;
    int _numRows;
    int _numColumns;
    int _exceptionsLength;
    int _dictionaryStart;
    int _dictionaryLength;
    int _dataSchemaStart;
    int _dataSchemaLength;
    int _fixedSizeDataStart;
    int _fixedSizeDataLength;
    int _variableSizeDataStart;
    int _variableSizeDataLength;
    int _metadataStart;

    public Header(int firstInt, int numRows, int numColumns) {
      _firstInt = firstInt;
      _numRows = numRows;
      _numColumns = numColumns;
    }

    public Header(int firstInt, int numRows, int numColumns, int exceptionsLength, int dictionaryStart,
        int dictionaryLength, int dataSchemaStart, int dataSchemaLength, int fixedSizeDataStart,
        int fixedSizeDataLength, int variableSizeDataStart, int variableSizeDataLength, int metadataStart) {
      _firstInt = firstInt;
      _numRows = numRows;
      _numColumns = numColumns;
      _exceptionsLength = exceptionsLength;
      _dictionaryStart = dictionaryStart;
      _dictionaryLength = dictionaryLength;
      _dataSchemaStart = dataSchemaStart;
      _dataSchemaLength = dataSchemaLength;
      _fixedSizeDataStart = fixedSizeDataStart;
      _fixedSizeDataLength = fixedSizeDataLength;
      _variableSizeDataStart = variableSizeDataStart;
      _variableSizeDataLength = variableSizeDataLength;
      _metadataStart = metadataStart;
    }

    public void serialize(ByteBuffer into) {
      Preconditions.checkState(into.remaining() >= BYTES, "Buffer does not have enough space to "
          + "serialize the header");
      into.putInt(_firstInt);
      into.putInt(_numRows);
      into.putInt(_numColumns);

      // offsets are stored first and we need to add the header offset
      into.putInt(BYTES); // exceptions start
      into.putInt(_exceptionsLength);
      into.putInt(_dictionaryStart); // dictionary start
      into.putInt(_dictionaryLength); // dictionary length
      into.putInt(_dataSchemaStart);
      into.putInt(_dataSchemaLength);
      into.putInt(_fixedSizeDataStart);
      into.putInt(_fixedSizeDataLength);
      into.putInt(_variableSizeDataStart);
      into.putInt(_variableSizeDataLength);
    }

    public static Header deserialize(DataInput input)
        throws IOException {
      int firstInt = input.readInt();
      int numRows = input.readInt();
      int numCols = input.readInt();

      input.skipBytes(Integer.BYTES); // exceptionsStart
      int exceptionsLength = input.readInt();
      int dictionaryStart = input.readInt();
      int dictionaryLength = input.readInt();
      int dataSchemaStart = input.readInt();
      int dataSchemaLength = input.readInt();
      int fixedSizeDataStart = input.readInt();
      int fixedSizeDataLength = input.readInt();
      int variableSizeDataStart = input.readInt();
      int variableSizeDataLength = input.readInt();
      int metadataStart = variableSizeDataStart + variableSizeDataLength;
      return new Header(firstInt, numRows, numCols, exceptionsLength, dictionaryStart, dictionaryLength,
          dataSchemaStart, dataSchemaLength, fixedSizeDataStart, fixedSizeDataLength, variableSizeDataStart,
          variableSizeDataLength, metadataStart);
    }

    @Override
    public String toString() {
      return "Header{" + "_numRows=" + _numRows + ", _numColumns=" + _numColumns + ", _exceptionsLength="
          + _exceptionsLength + ", _dictionaryStart=" + _dictionaryStart + ", _dictionaryLength=" + _dictionaryLength
          + ", _dataSchemaStart=" + _dataSchemaStart + ", _dataSchemaLength=" + _dataSchemaLength
          + ", _fixedSizeDataStart=" + _fixedSizeDataStart + ", _fixedSizeDataLength=" + _fixedSizeDataLength
          + ", _variableSizeDataStart=" + _variableSizeDataStart + ", _variableSizeDataLength="
          + _variableSizeDataLength + ", _metadataStart=" + _metadataStart + '}';
    }

    public void updateStarts() {
      _dictionaryStart = BYTES + _exceptionsLength;
      _dataSchemaStart = _dictionaryStart + _dictionaryLength;
      _fixedSizeDataStart = _dataSchemaStart + _dataSchemaLength;
      _variableSizeDataStart = _fixedSizeDataStart + _fixedSizeDataLength;
      _metadataStart = _variableSizeDataStart + _variableSizeDataLength;
    }

    public int getFirstInt() {
      return _firstInt;
    }

    public int getNumRows() {
      return _numRows;
    }

    public int getNumColumns() {
      return _numColumns;
    }

    public int getExceptionsStart() {
      return Header.BYTES; // exceptions are always codified after the end of the header
    }

    public int getExceptionsLength() {
      return _exceptionsLength;
    }

    public int getDictionaryStart() {
      return _dictionaryStart;
    }

    public int getDictionaryLength() {
      return _dictionaryLength;
    }

    public int getDataSchemaStart() {
      return _dataSchemaStart;
    }

    public int getDataSchemaLength() {
      return _dataSchemaLength;
    }

    public int getFixedSizeDataStart() {
      return _fixedSizeDataStart;
    }

    public int getFixedSizeDataLength() {
      return _fixedSizeDataLength;
    }

    public int getVariableSizeDataStart() {
      return _variableSizeDataStart;
    }

    public int getVariableSizeDataLength() {
      return _variableSizeDataLength;
    }

    public int getMetadataStart() {
      return _metadataStart;
    }
  }
}
