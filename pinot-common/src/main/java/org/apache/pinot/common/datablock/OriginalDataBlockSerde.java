package org.apache.pinot.common.datablock;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.output.UnsynchronizedByteArrayOutputStream;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.segment.spi.memory.PagedPinotOutputStream;
import org.apache.pinot.segment.spi.memory.PinotInputStream;
import org.apache.pinot.segment.spi.memory.PinotOutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;


public class OriginalDataBlockSerde implements DataBlockSerde {

  private final PagedPinotOutputStream.PageAllocator _allocator;

  public OriginalDataBlockSerde() {
    _allocator = PagedPinotOutputStream.HeapPageAllocator.createSmall();
  }

  public OriginalDataBlockSerde(PagedPinotOutputStream.PageAllocator allocator) {
    _allocator = allocator;
  }

  public DataBuffer serialize(DataBlock.Raw dataBlock, int firstInt)
      throws IOException {
    try (PagedPinotOutputStream into = new PagedPinotOutputStream(_allocator)) {
      into.writeInt(firstInt);
      into.writeInt(dataBlock.getNumberOfRows());
      into.writeInt(dataBlock.getNumberOfColumns());
      int dataOffset = Integer.BYTES * 13;

      // Write exceptions section offset(START|SIZE).
      into.writeInt(dataOffset);
      byte[] exceptionsBytes;
      exceptionsBytes = serializeExceptions(dataBlock);
      into.writeInt(exceptionsBytes.length);
      dataOffset += exceptionsBytes.length;

      // Write dictionary map section offset(START|SIZE).
      into.writeInt(dataOffset);
      byte[] dictionaryBytes = null;
      if (dataBlock.getStringDictionary() != null) {
        dictionaryBytes = serializeStringDictionary(dataBlock, into);
        into.writeInt(dictionaryBytes.length);
        dataOffset += dictionaryBytes.length;
      } else {
        into.writeInt(0);
      }

      // Write data schema section offset(START|SIZE).
      DataSchema dataSchema = dataBlock.getDataSchema();
      into.writeInt(dataOffset);
      byte[] dataSchemaBytes = null;
      if (dataSchema != null) {
        dataSchemaBytes = dataSchema.toBytes();
        into.writeInt(dataSchemaBytes.length);
        dataOffset += dataSchemaBytes.length;
      } else {
        into.writeInt(0);
      }

      // Write fixed size data section offset(START|SIZE).
      into.writeInt(dataOffset);
      DataBuffer fixedData = dataBlock.getFixedData();
      if (fixedData != null) {
        into.writeInt((int) fixedData.size());
        dataOffset += (int) fixedData.size();
      } else {
        into.writeInt(0);
      }

      // Write variable size data section offset(START|SIZE).
      into.writeInt(dataOffset);
      DataBuffer variableSizeData = dataBlock.getVarSizeData();
      if (variableSizeData != null) {
        into.writeInt((int) variableSizeData.size());
      } else {
        into.writeInt(0);
      }

      // Write actual data.
      // Write exceptions bytes.
      into.write(exceptionsBytes);
      // Write dictionary map bytes.
      if (dictionaryBytes != null) {
        into.write(dictionaryBytes);
      }
      // Write data schema bytes.
      if (dataSchemaBytes != null) {
        into.write(dataSchemaBytes);
      }
      // Write fixed size data bytes.
      if (fixedData != null) {
        into.write(fixedData);
      }
      // Write variable size data bytes.
      if (variableSizeData != null) {
        into.write(variableSizeData);
      }

      serializeMetadata(dataBlock, into);

      return into.asBuffer(ByteOrder.BIG_ENDIAN, false);
    }
  }

  private byte[] serializeStringDictionary(DataBlock.Raw block, PinotOutputStream output)
      throws IOException {
    String[] stringDictionary = block.getStringDictionary();
    if (stringDictionary.length == 0) {
      return new byte[4];
    }
    UnsynchronizedByteArrayOutputStream byteArrayOutputStream = new UnsynchronizedByteArrayOutputStream(1024);
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    dataOutputStream.writeInt(stringDictionary.length);
    for (String entry : stringDictionary) {
      byte[] valueBytes = entry.getBytes(UTF_8);
      dataOutputStream.writeInt(valueBytes.length);
      dataOutputStream.write(valueBytes);
    }

    return byteArrayOutputStream.toByteArray();
  }

  private byte[] serializeExceptions(DataBlock.Raw dataBlock)
      throws IOException {
    Map<Integer, String> errCodeToExceptionMap = dataBlock.getExceptions();
    if (errCodeToExceptionMap.isEmpty()) {
      return new byte[4];
    }
    UnsynchronizedByteArrayOutputStream byteArrayOutputStream = new UnsynchronizedByteArrayOutputStream(1024);
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    dataOutputStream.writeInt(errCodeToExceptionMap.size());

    for (Map.Entry<Integer, String> entry : errCodeToExceptionMap.entrySet()) {
      int key = entry.getKey();
      String value = entry.getValue();
      byte[] valueBytes = value.getBytes(UTF_8);
      dataOutputStream.writeInt(key);
      dataOutputStream.writeInt(valueBytes.length);
      dataOutputStream.write(valueBytes);
    }

    return byteArrayOutputStream.toByteArray();
  }

  @Override
  public DataBlock deserialize(DataBuffer buffer, long offset, DataBlock.Type type)
      throws IOException {
    try (PinotInputStream stream = buffer.openInputStream(offset)) {
      // Read header.
      stream.skipBytes(Integer.BYTES); // Skip the version and type.
      int numRows = stream.readInt();
      stream.skipBytes(Integer.BYTES); // Skip the number of columns.
      int exceptionsStart = stream.readInt();
      int exceptionsLength = stream.readInt();
      int dictionaryMapStart = stream.readInt();
      int dictionaryMapLength = stream.readInt();
      int dataSchemaStart = stream.readInt();
      int dataSchemaLength = stream.readInt();
      int fixedSizeDataStart = stream.readInt();
      int fixedSizeDataLength = stream.readInt();
      int variableSizeDataStart = stream.readInt();
      int variableSizeDataLength = stream.readInt();

      // Read exceptions.
      Map<Integer, String> errCodeToExceptionMap;
      if (exceptionsLength != 0) {
        stream.seek(exceptionsStart);
        errCodeToExceptionMap = deserializeExceptions(stream);
      } else {
        errCodeToExceptionMap = new HashMap<>();
      }

      // Read dictionary.
      String[] stringDictionary;
      if (dictionaryMapLength != 0) {
        stream.seek(dictionaryMapStart);
        stringDictionary = deserializeStringDictionary(stream);
      } else {
        stringDictionary = null;
      }

      // Read data schema.
      DataSchema dataSchema;
      if (dataSchemaLength != 0) {
        stream.seek(dataSchemaStart);
        dataSchema = DataSchema.fromBytes(stream);
      } else {
        dataSchema = null;
      }

      // Read fixed size data.
      byte[] fixedSizeDataBytes;
      if (fixedSizeDataLength != 0) {
        fixedSizeDataBytes = new byte[fixedSizeDataLength];
        stream.seek(fixedSizeDataStart);
        stream.readFully(fixedSizeDataBytes);
      } else {
        fixedSizeDataBytes = null;
      }

      // Read variable size data.
      byte[] variableSizeDataBytes = new byte[variableSizeDataLength];
      if (variableSizeDataLength != 0) {
        stream.seek(variableSizeDataStart);
        stream.readFully(variableSizeDataBytes);
      }

      switch (type) {
        case COLUMNAR:
          return new ColumnarDataBlock(numRows, dataSchema, stringDictionary, fixedSizeDataBytes,
              variableSizeDataBytes);
        case ROW:
          return new RowDataBlock(numRows, dataSchema, stringDictionary, fixedSizeDataBytes, variableSizeDataBytes);
        case METADATA:
          List<DataBuffer> metadata = deserializeMetadata(buffer, stream.getCurrentOffset());
          if (errCodeToExceptionMap.isEmpty()) {
            return new MetadataBlock(metadata);
          } else {
            return MetadataBlock.newError(errCodeToExceptionMap);
          }
        default:
          throw new IllegalStateException();
      }
    }
  }

  private List<DataBuffer> deserializeMetadata(DataBuffer buffer, long startOffset) {
    long currentOffset = startOffset;
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

  private String[] deserializeStringDictionary(PinotInputStream stream)
      throws IOException {
    int dictionarySize = stream.readInt();
    String[] stringDictionary = new String[dictionarySize];
    for (int i = 0; i < dictionarySize; i++) {
      stringDictionary[i] = stream.readInt4UTF();
    }
    return stringDictionary;
  }

  private Map<Integer, String> deserializeExceptions(PinotInputStream stream)
      throws IOException {
    int numExceptions = stream.readInt();
    Map<Integer, String> exceptions = new HashMap<>(HashUtil.getHashMapCapacity(numExceptions));
    for (int i = 0; i < numExceptions; i++) {
      int errCode = stream.readInt();
      String errMessage = stream.readInt4UTF();
      exceptions.put(errCode, errMessage);
    }
    return exceptions;
  }

  private void serializeMetadata(BaseDataBlock.Raw dataBlock, PinotOutputStream output)
      throws IOException {
    if (!(dataBlock instanceof MetadataBlock)) {
      output.writeInt(0);
      return;
    }
    List<DataBuffer> statsByStage = dataBlock.getStatsByStage();
    if (statsByStage == null) {
      output.writeInt(0);
      return;
    }
    int size = statsByStage.size();
    output.writeInt(size);
    if (size > 0) {
      for (DataBuffer stat : statsByStage) {
        if (stat == null) {
          output.writeBoolean(false);
        } else {
          output.writeBoolean(true);
          if (stat.size() > Integer.MAX_VALUE) {
            throw new IOException("Stat size is too large to serialize");
          }
          output.writeInt((int) stat.size());
          output.write(stat);
        }
      }
    }
  }

  @Override
  public Version getVersion() {
    return Version.V2;
  }
}
