/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.utils;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.common.utils.DataTableBuilder.DataSchema;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * Read only Datatable. Use DataTableBuilder to build the data table
 */
public class DataTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataTable.class);
  private static final Charset UTF8 = Charset.forName("UTF-8");

  public static final String EXCEPTION_METADATA_KEY = "Exception";
  public static final String NUM_DOCS_SCANNED_METADATA_KEY = "numDocsScanned";
  public static final String NUM_ENTRIES_SCANNED_IN_FILTER_METADATA_KEY = "numEntriesScannedInFilter";
  public static final String NUM_ENTRIES_SCANNED_POST_FILTER_METADATA_KEY = "numEntriesScannedPostFilter";
  public static final String TOTAL_DOCS_METADATA_KEY = "totalDocs";
  public static final String TIME_USED_MS_METADATA_KEY = "timeUsedMs";
  public static final String TRACE_INFO_METADATA_KEY = "traceInfo";
  public static final String REQUEST_ID_METADATA_KEY = "requestId";

  // Data Table version
  public enum Version {
    V1(1), // Keep the value of '1' for backward compatibility
    V2(2);

    private int value;

    Version(int value) {
      this.value = value;
    }

    public static Version valueOf(int versionNum) {
      // Only two elements, so OK to linear search, v.s. overhead of maintaining & looking up map.
      for (Version version : values()) {
        if (version.value == versionNum) {
          return version;
        }
      }
      throw new IllegalArgumentException("Illegal value for version " + versionNum);
    }

    public int getValue() {
      return value;
    }
  }

  private final DataTableSerDe dataTableSerDe;
  private final Version version;

  int numRows;

  int numCols;

  DataSchema schema;

  private Map<String, Map<Integer, String>> dictionary;

  private Map<String, String> metadata;

  private ByteBuffer fixedSizeData;

  private ByteBuffer variableSizeData;

  private int[] columnOffsets;

  private int rowSizeInBytes;

  private byte[] fixedSizeDataBytes;

  private byte[] variableSizeDataBytes;

  /**
   *
   * @param numRows
   * @param dictionary
   * @param metadata
   * @param schema
   * @param fixedSizeDataBytes
   * @param variableSizeDataBytes
   * @throws Exception
   */
  public DataTable(Version version, int numRows, Map<String, Map<Integer, String>> dictionary,
      Map<String, String> metadata, DataSchema schema, byte[] fixedSizeDataBytes, byte[] variableSizeDataBytes)
      throws Exception {
    this.dataTableSerDe = DataTableSerDeRegistry.getInstance().get();
    this.version = version;
    this.numRows = numRows;
    this.dictionary = dictionary;
    this.metadata = metadata;
    this.schema = schema;
    this.fixedSizeDataBytes = fixedSizeDataBytes;
    this.variableSizeDataBytes = variableSizeDataBytes;
    numCols = schema.columnNames.length;
    fixedSizeData = ByteBuffer.wrap(fixedSizeDataBytes);
    variableSizeData = ByteBuffer.wrap(variableSizeDataBytes);
    columnOffsets = computeColumnOffsets(schema);
  }

  /**
   *
   * @param metadata
   */
  public DataTable(Map<String, String> metadata) {
    dataTableSerDe = DataTableSerDeRegistry.getInstance().get();
    this.version = deriveVersionFromDataTableSerDe(dataTableSerDe);
    this.metadata = metadata;
  }

  /**
   *
   * @param schema
   * @return
   */
  private int[] computeColumnOffsets(DataSchema schema) {
    if (schema == null) {
      return null;
    }
    final int[] columnOffsets = new int[schema.columnNames.length];
    for (int i = 0; i < schema.columnNames.length; i++) {
      final com.linkedin.pinot.common.data.FieldSpec.DataType type = schema.columnTypes[i];
      columnOffsets[i] = rowSizeInBytes;
      switch (type) {
        case BOOLEAN:
          rowSizeInBytes += 1;
          break;
        case BYTE:
          rowSizeInBytes += 1;
          break;
        case CHAR:
          rowSizeInBytes += 2;
          break;
        case SHORT:
          rowSizeInBytes += 2;
          break;
        case INT:
          rowSizeInBytes += 4;
          break;
        case LONG:
          rowSizeInBytes += 8;
          break;
        case FLOAT:
          rowSizeInBytes += 8;
          break;
        case DOUBLE:
          rowSizeInBytes += 8;
          break;
        case STRING:
          rowSizeInBytes += 4;
          break;
        case OBJECT:
          rowSizeInBytes += 8;
          break;
        case BYTE_ARRAY:
        case CHAR_ARRAY:
        case INT_ARRAY:
        case LONG_ARRAY:
        case FLOAT_ARRAY:
        case SHORT_ARRAY:
        case DOUBLE_ARRAY:
        case STRING_ARRAY:
          rowSizeInBytes += 8;
          break;

        default:
          throw new RuntimeException("Unsupported datatype:" + type);
      }
    }
    return columnOffsets;
  }

  /**
   *
   * @param buffer
   */
  public DataTable(byte[] buffer) {
    final ByteBuffer input = ByteBuffer.wrap(buffer);
    dataTableSerDe = DataTableSerDeRegistry.getInstance().get();

    // Assert that version can be de-serialized.
    version = Version.valueOf(input.getInt());
    deserializeDataTable(input);
  }

  private void deserializeDataTable(ByteBuffer input) {
    numRows = input.getInt();
    numCols = input.getInt();
    // READ dictionary
    final int dictionaryStart = input.getInt();
    final int dictionaryLength = input.getInt();
    final int metadataStart = input.getInt();
    final int metadataLength = input.getInt();
    final int schemaStart = input.getInt();
    final int schemaLength = input.getInt();
    final int fixedDataStart = input.getInt();
    final int fixedDataLength = input.getInt();
    final int variableDataStart = input.getInt();
    final int variableDataLength = input.getInt();

    // READ DICTIONARY

    byte[] dictionaryBytes = null;
    if (dictionaryLength != 0) {
      dictionaryBytes = new byte[dictionaryLength];
      input.position(dictionaryStart);
      input.get(dictionaryBytes);
      dictionary = (Map<String, Map<Integer, String>>) deserializeDictionary(dictionaryBytes);
    } else {
      dictionary = new HashMap<String, Map<Integer, String>>(1);
    }

    // READ METADATA
    byte[] metadataBytes;
    if (metadataLength != 0) {
      metadataBytes = new byte[metadataLength];
      input.position(metadataStart);
      input.get(metadataBytes);
      metadata = (Map<String, String>) deserializeMetadata(metadataBytes);
    } else {
      metadata = new HashMap<String, String>();
    }

    // READ SCHEMA
    byte[] schemaBytes;

    if (schemaLength != 0) {
      schemaBytes = new byte[schemaLength];
      input.position(schemaStart);
      input.get(schemaBytes);
      schema = DataSchema.fromBytes(schemaBytes);
      columnOffsets = computeColumnOffsets(schema);
    }

    // READ FIXED SIZE DATA BYTES
    if (fixedDataLength != 0) {
      fixedSizeDataBytes = new byte[fixedDataLength];
      input.position(fixedDataStart);
      input.get(fixedSizeDataBytes);
      fixedSizeData = ByteBuffer.wrap(fixedSizeDataBytes);
    }

    // READ VARIABLE SIZE DATA BYTES
    if (variableDataLength != 0) {
      variableSizeDataBytes = new byte[variableDataLength];
      input.position(variableDataStart);
      input.get(variableSizeDataBytes);
      variableSizeData = ByteBuffer.wrap(variableSizeDataBytes);
    }
  }

  public DataTable() {
    // Used for empty results.
    dataTableSerDe = DataTableSerDeRegistry.getInstance().get();
    version = deriveVersionFromDataTableSerDe(dataTableSerDe);
    metadata = new HashMap<>();
  }

  /**
   * Helper method to derive version based on the registered DataTableSer/de.
   * <p> - Version is derived to be V1 if DataTableJavaSerDe is registered.</p>
   * <p> - Checks for class equality instead of 'instanceof' as other ser/de's can be derived
   *       from {@link DataTableJavaSerDe}</p>
   * @return
   */
  public static Version deriveVersionFromDataTableSerDe(DataTableSerDe dataTableSerDe) {
    return (dataTableSerDe.getClass().equals(DataTableJavaSerDe.class)) ? Version.V1
        : Version.V2;
  }

  /**
   * Serialize the data table into a byte-array, using {@ref #Version.V1}
   *
   * @return Serialized byte-array
   * @throws Exception
   */
  public  byte[] toBytes() throws Exception {
    return toBytes(version);
  }

  /**
   * Serialize the data table into a byte-array, as per the specified serialization.
   *
   * @param version Format version to use for serialization.
   * @return Serialized byte-array
   * @throws Exception
   */
  public byte[] toBytes(Version version) throws Exception {
    final byte[] dictionaryBytes = serializeDictionary();
    final byte[] metadataBytes = serializeMetadata();
    byte[] schemaBytes = new byte[0];
    if (schema != null) {
      schemaBytes = schema.toBytes();
    }
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final DataOutputStream out = new DataOutputStream(baos);
    // TODO: convert this format into a proper class
    // VERSION|NUM_ROW|NUM_COL|(START|SIZE) -- START|SIZE 5 PAIRS FOR
    // DICTIONARY, METADATA,
    // SCHEMA, DATATABLE, VARIABLE DATA BUFFER --> 4 + 4 + 4 + 5*8 = 52
    // bytes

    out.writeInt(version.getValue());
    out.writeInt(numRows);
    out.writeInt(numCols);
    // dictionary
    int baseOffset = 52;
    out.writeInt(baseOffset);
    out.writeInt(dictionaryBytes.length);
    baseOffset += dictionaryBytes.length;

    // metadata
    out.writeInt(baseOffset);
    out.writeInt(metadataBytes.length);
    baseOffset += metadataBytes.length;

    // schema
    out.writeInt(baseOffset);
    out.writeInt(schemaBytes.length);
    baseOffset += schemaBytes.length;

    // datatable
    out.writeInt(baseOffset);
    if (fixedSizeDataBytes == null) {
      out.writeInt(0);
    } else {
      out.writeInt(fixedSizeDataBytes.length);
      baseOffset += fixedSizeDataBytes.length;
    }

    // variable data
    out.writeInt(baseOffset);
    if (variableSizeDataBytes == null) {
      out.writeInt(0);
    } else {
      out.writeInt(variableSizeDataBytes.length);
    }

    // write them
    out.write(dictionaryBytes);
    out.write(metadataBytes);
    out.write(schemaBytes);
    if (fixedSizeDataBytes != null) {
      out.write(fixedSizeDataBytes);
    }
    if (variableSizeDataBytes != null) {
      out.write(variableSizeDataBytes);
    }
    byte[] byteArray = baos.toByteArray();
    return byteArray;
  }

  private byte[] serializeMetadata() throws Exception {
    if (metadata != null) {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataOutputStream out = new DataOutputStream(baos);
      out.writeInt(metadata.size());
      for (Entry<String, String> entry : metadata.entrySet()) {
        byte[] keyBytes = entry.getKey().getBytes(UTF8);
        out.writeInt(keyBytes.length);
        out.write(keyBytes);
        byte[] valueBytes = entry.getValue().getBytes(UTF8);
        out.writeInt(valueBytes.length);
        out.write(valueBytes);
      }
      return baos.toByteArray();
    }
    return new byte[0];
  }

  private Map<String, String> deserializeMetadata(byte[] buffer) {
    Map<String, String> map = new HashMap<String, String>();
    try {
      final ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
      final DataInputStream in = new DataInputStream(bais);
      int size = in.readInt();
      for (int i = 0; i < size; i++) {
        Integer keyLength = in.readInt();
        byte[] keyBytes = new byte[keyLength];
        in.read(keyBytes);
        int valueLength = in.readInt();
        byte[] valueBytes = new byte[valueLength];
        in.read(valueBytes);
        map.put(new String(keyBytes, UTF8), new String(valueBytes, UTF8));
      }
    } catch (Exception e) {
      LOGGER.error("Exception while deserializing dictionary", e);
    }
    return map;
  }

  private byte[] serializeDictionary() throws Exception {
    if (dictionary != null) {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataOutputStream out = new DataOutputStream(baos);
      out.writeInt(dictionary.size());
      for (String key : dictionary.keySet()) {
        byte[] bytes = key.getBytes(UTF8);
        out.writeInt(bytes.length);
        out.write(bytes);
        Map<Integer, String> map = dictionary.get(key);
        out.writeInt(map.size());
        for (Entry<Integer, String> entry : map.entrySet()) {
          out.writeInt(entry.getKey());
          byte[] valueBytes = entry.getValue().getBytes(UTF8);
          out.writeInt(valueBytes.length);
          out.write(valueBytes);
        }
      }
      return baos.toByteArray();
    }
    return new byte[0];
  }

  private Map<String, Map<Integer, String>> deserializeDictionary(byte[] buffer) {
    Map<String, Map<Integer, String>> map = new HashMap<String, Map<Integer, String>>();
    try {
      final ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
      final DataInputStream in = new DataInputStream(bais);
      int size = in.readInt();
      byte[] temp;
      for (int i = 0; i < size; i++) {
        int readLength = in.readInt();
        temp = new byte[readLength];
        in.read(temp);
        Map<Integer, String> childMap = new HashMap<Integer, String>();
        map.put(new String(temp, UTF8), childMap);
        int childMapSize = in.readInt();
        for (int j = 0; j < childMapSize; j++) {
          Integer key = in.readInt();
          int valueLength = in.readInt();
          temp = new byte[valueLength];
          in.read(temp);
          childMap.put(key, new String(temp, UTF8));
        }
      }
    } catch (Exception e) {
      LOGGER.error("Exception while deserializing dictionary", e);
    }
    return map;
  }

  /**
   *
   * @param value
   * @return
   */
  private byte[] serializeObject(Object value) {
    byte[] bytes;
    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutput out = null;

    try {
      try {
        out = new ObjectOutputStream(bos);
        out.writeObject(value);
      } catch (final IOException e) {
        LOGGER.error("Caught exception", e);
        Utils.rethrowException(e);
      }
      bytes = bos.toByteArray();

    } finally {
      IOUtils.closeQuietly((Closeable) out);
      IOUtils.closeQuietly(bos);
    }
    return bytes;
  }

  /**
   *
   * @return
   */
  public int getNumberOfRows() {
    return numRows;
  }

  /**
   *
   * @return
   */
  public int getNumberOfCols() {
    return numCols;
  }

  /**
   *
   * @return
   */
  public DataSchema getDataSchema() {
    return schema;
  }

  /**
   *
   * @param rowId
   * @param colId
   * @return
   */
  public boolean getBoolean(int rowId, int colId) {
    fixedSizeData.position(rowId * rowSizeInBytes + columnOffsets[colId]);
    return (byte) 1 == fixedSizeData.get();
  }

  /**
   *
   * @param rowId
   * @param colId
   * @return
   */
  public char getChar(int rowId, int colId) {
    fixedSizeData.position(rowId * rowSizeInBytes + columnOffsets[colId]);
    return fixedSizeData.getChar();
  }

  /**
   *
   * @param rowId
   * @param colId
   * @return
   */
  public byte getByte(int rowId, int colId) {
    fixedSizeData.position(rowId * rowSizeInBytes + columnOffsets[colId]);
    return fixedSizeData.get();
  }

  /**
   *
   * @param rowId
   * @param colId
   * @return
   */
  public short getShort(int rowId, int colId) {
    fixedSizeData.position(rowId * rowSizeInBytes + columnOffsets[colId]);
    return fixedSizeData.getShort();
  }

  /**
   *
   * @param rowId
   * @param colId
   * @return
   */
  public int getInt(int rowId, int colId) {
    fixedSizeData.position(rowId * rowSizeInBytes + columnOffsets[colId]);
    return fixedSizeData.getInt();
  }

  /**
   *
   * @param rowId
   * @param colId
   * @return
   */
  public long getLong(int rowId, int colId) {
    fixedSizeData.position(rowId * rowSizeInBytes + columnOffsets[colId]);
    return fixedSizeData.getLong();
  }

  /**
   *
   * @param rowId
   * @param colId
   * @return
   */
  public float getFloat(int rowId, int colId) {
    fixedSizeData.position(rowId * rowSizeInBytes + columnOffsets[colId]);
    return fixedSizeData.getFloat();
  }

  /**
   *
   * @param rowId
   * @param colId
   * @return
   */
  public double getDouble(int rowId, int colId) {
    fixedSizeData.position(rowId * rowSizeInBytes + columnOffsets[colId]);
    return fixedSizeData.getDouble();
  }

  /**
   *
   * @param rowId
   * @param colId
   * @return
   */
  public String getString(int rowId, int colId) {
    fixedSizeData.position(rowId * rowSizeInBytes + columnOffsets[colId]);
    final int id = fixedSizeData.getInt();
    final Map<Integer, String> map = dictionary.get(schema.columnNames[colId]);
    return map.get(id);
  }

  /**
   *
   * @param rowId
   * @param colId
   * @return
   */
  public byte[] getByteArray(int rowId, int colId) {
    final int size = positionCursorInVariableBuffer(rowId, colId);
    byte[] ret = new byte[size];
    for (int i = 0; i < size; i++) {
      ret[i] = variableSizeData.get();
    }
    return ret;
  }

  /**
   *
   * @param rowId
   * @param colId
   * @return
   */
  public char[] getCharArray(int rowId, int colId) {
    final int size = positionCursorInVariableBuffer(rowId, colId);
    char[] ret = new char[size];
    for (int i = 0; i < size; i++) {
      ret[i] = variableSizeData.getChar();
    }
    return ret;
  }

  /**
   *
   * @param rowId
   * @param colId
   * @return
   */
  public short[] getShortArray(int rowId, int colId) {
    final int size = positionCursorInVariableBuffer(rowId, colId);
    short[] ret = new short[size];
    for (int i = 0; i < size; i++) {
      ret[i] = variableSizeData.getShort();
    }
    return ret;
  }

  /**
   *
   * @param rowId
   * @param colId
   * @return
   */
  public int[] getIntArray(int rowId, int colId) {
    final int size = positionCursorInVariableBuffer(rowId, colId);
    int[] ret = new int[size];
    for (int i = 0; i < size; i++) {
      ret[i] = variableSizeData.getInt();
    }
    return ret;
  }

  /**
   *
   * @param rowId
   * @param colId
   * @return
   */
  public long[] getLongArray(int rowId, int colId) {
    final int size = positionCursorInVariableBuffer(rowId, colId);
    long[] ret = new long[size];
    for (int i = 0; i < size; i++) {
      ret[i] = variableSizeData.getLong();
    }
    return ret;
  }

  /**
   *
   * @param rowId
   * @param colId
   * @return
   */
  public float[] getFloatArray(int rowId, int colId) {
    final int size = positionCursorInVariableBuffer(rowId, colId);
    float[] ret = new float[size];
    for (int i = 0; i < size; i++) {
      ret[i] = variableSizeData.getFloat();
    }
    return ret;
  }

  /**
   *
   * @param rowId
   * @param colId
   * @return
   */
  public double[] getDoubleArray(int rowId, int colId) {
    final int size = positionCursorInVariableBuffer(rowId, colId);
    double[] ret = new double[size];
    for (int i = 0; i < size; i++) {
      ret[i] = variableSizeData.getDouble();
    }
    return ret;
  }

  /**
   *
   * @param rowId
   * @param colId
   * @return
   */
  public String[] getStringArray(int rowId, int colId) {
    final int size = positionCursorInVariableBuffer(rowId, colId);
    String[] ret = new String[size];
    final Map<Integer, String> map = dictionary.get(schema.columnNames[colId]);

    for (int i = 0; i < size; i++) {
      ret[i] = map.get(variableSizeData.getInt());
    }
    return ret;
  }

  /**
   *
   * @param rowId
   * @param colId
   * @return
   */
  private int positionCursorInVariableBuffer(int rowId, int colId) {
    int pos = rowId * rowSizeInBytes + columnOffsets[colId];
    fixedSizeData.position(pos);
    final int position = fixedSizeData.getInt();
    final int size = fixedSizeData.getInt();
    variableSizeData.position(position);
    return size;
  }

  /**
   *
   * @param rowId
   * @param colId
   * @return
   */
  @SuppressWarnings("unchecked")
  public <T extends Serializable> T getObject(int rowId, int colId) {
    final int length = positionCursorInVariableBuffer(rowId, colId);

    DataTableSerDe.DataType dataType = DataTableSerDe.DataType.Object;
    if (version == Version.V2) {
      dataType = DataTableSerDe.DataType.valueOf(variableSizeData.getInt());
    }

    final byte[] serData = new byte[length];
    variableSizeData.get(serData);
    return (T) dataTableSerDe.deserialize(serData, dataType);
  }

  /**
   *
   * @return
   */
  public Map<String, String> getMetadata() {
    return metadata;
  }

  /**
   * To string representation of datatable, contains the content of fixed data
   * size buffer
   */
  @Override
  public String toString() {
    if (schema == null) {
      return metadata.toString();
    }
    final StringBuilder b = new StringBuilder();
    b.append(schema.toString());
    b.append("\n");

    b.append("numRows : " + numRows + "\n");
    fixedSizeData.position(0);
    for (int rowId = 0; rowId < numRows; rowId++) {
      for (int colId = 0; colId < numCols; colId++) {
        final com.linkedin.pinot.common.data.FieldSpec.DataType type = schema.columnTypes[colId];
        switch (type) {
          case BOOLEAN:
            b.append(fixedSizeData.get());
            break;
          case BYTE:
            b.append(fixedSizeData.get());
            break;
          case CHAR:
            b.append(fixedSizeData.getChar());
            break;
          case SHORT:
            b.append(fixedSizeData.getShort());
            break;
          case INT:
            b.append(fixedSizeData.getInt());
            break;
          case LONG:
            b.append(fixedSizeData.getLong());
            break;
          case FLOAT:
            b.append(fixedSizeData.getFloat());
            break;
          case DOUBLE:
            b.append(fixedSizeData.getDouble());
            break;
          case STRING:
            b.append(fixedSizeData.getInt());
            break;
          case OBJECT:
            b.append(String.format("(%s:%s)", fixedSizeData.getInt(), fixedSizeData.getInt()));
            break;
          case BYTE_ARRAY:
          case CHAR_ARRAY:
          case SHORT_ARRAY:
          case INT_ARRAY:
          case LONG_ARRAY:
          case FLOAT_ARRAY:
          case DOUBLE_ARRAY:
          case STRING_ARRAY:
            b.append(String.format("(%s:%s)", fixedSizeData.getInt(), fixedSizeData.getInt()));
            break;
          default:
            throw new RuntimeException("Unsupported datatype:" + type);
        }
        b.append("\t");
      }
      b.append("\n");
    }
    return b.toString();
  }

  public void addException(ProcessingException exception) {
    metadata.put(EXCEPTION_METADATA_KEY + exception.getErrorCode(), exception.getMessage());
  }
}
