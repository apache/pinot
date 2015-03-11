/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.linkedin.pinot.common.data.FieldSpec.DataType;


/**
 *
 * Datatable that holds data in a matrix form. The purpose of this class is to
 * provide a way to construct a datatable and ability to serialize and
 * deserialize.<br/>
 * Why can't we use existing serialization/deserialization mechanism. Most
 * existing techniques protocol buffer, thrift, avro are optimized for
 * transporting a single record but Pinot transfers quite a lot of data from
 * server to broker during the scatter/gather operation. The cost of
 * serialization and deserialization directly impacts the performance. Most
 * ser/deser requires us to convert the primitives data types in objects like
 * Integer etc. This is waste of cpu resource and increase the payload size. We
 * optimize the data format for Pinot usecase. We can also support lazy
 * construction of obejcts. Infact we retain the bytes as it is and will be able
 * to lookup the a field directly within a byte buffer.<br/>
 *
 * USAGE:
 *
 * Datatable is initialized with the schema of the table. Schema describes the
 * columnnames, their order and data type for each column.<br/>
 * Each row must follow the same convention. We don't support MultiValue columns
 * for now. Format,
 * |VERSION,DATA_START_OFFSET,DICTIONARY_START_OFFSET,INDEX_START_OFFSET
 * ,METADATA_START_OFFSET | |<DATA> |
 *
 * |<DICTIONARY>|
 *
 *
 * |<METADATA>| Data contains the actual values written by the application We
 * first write the entire data in its raw byte format. For example if you data
 * type is Int, it will write 4 bytes. For most data types that are fixed width,
 * we just write the raw data. For special cases like String, we create a
 * dictionary. Dictionary will be never exposed to the user. All conversions
 * will be done internally. In future, we might decide dynamically if dictionary
 * creation is needed, for now we will always create dictionaries for string
 * columns. During deserialization we will always load the dictionary
 * first.Overall having dictionary allow us to convert data table into a fixed
 * width matrix and thus allowing look up and easy traversal.
 *
 * @author kgopalak
 *
 */
public class DataTableBuilder {
  /**
   * Initialize the datatable with metadata
   */

  Map<String, Map<String, Integer>> dictionary;

  Map<String, Map<Integer, String>> reverseDictionary;

  Map<String, String> metadata;

  private DataSchema schema;

  private int currentRowId;

  /**
   * temporary data holder for the current row
   */
  private ByteBuffer currentRowData;

  int[] columnOffsets;

  int rowSizeInBytes;

  /**
   * format length of header. VERSION, <br/>
   * START_OFFSET LENGTH for each sub section
   *
   */

  ByteHolder header;

  /**
   * SUB SECTIONS
   */
  /*
   * METADATA that simply contains key, value pairs format
   * keylength|key|valuelength|value
   */
  ByteHolder metadataHolder;
  /**
   * Holds the schema info. start
   */
  ByteHolder dataSchemaHolder;

  ByteHolder fixedSizeDataHolder;

  /**
   * Holds data
   */
  ByteHolder variableSizeDataHolder;

  boolean isOpen = false;

  public DataTableBuilder(DataSchema schema) {
    this.schema = schema;
    this.metadata = new HashMap<String, String>();
    columnOffsets = new int[schema.columnNames.length];
    fixedSizeDataHolder = new ByteHolder();
    variableSizeDataHolder = new ByteHolder();
    for (int i = 0; i < schema.columnNames.length; i++) {
      DataType type = schema.columnTypes[i];
      columnOffsets[i] = rowSizeInBytes;
      switch (type) {
        case BOOLEAN:
          rowSizeInBytes += 1; // represent using 1 byte 1 is true 0 is false
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
          rowSizeInBytes += 8;// first 4 bytes represent the position in variable
                              // buffer and next 4 bytes represents the length
          break;
        case BYTE_ARRAY:
        case CHAR_ARRAY:
        case SHORT_ARRAY:
        case INT_ARRAY:
        case LONG_ARRAY:
        case FLOAT_ARRAY:
        case DOUBLE_ARRAY:
        case STRING_ARRAY:
          rowSizeInBytes += 8;// first 4 bytes represent the position in variable
                              // buffer and next 4 bytes represents the number of
                              // elements
          break;
        default:
          throw new RuntimeException("Unsupported datatype:" + type);
      }
    }
    dictionary = new HashMap<String, Map<String, Integer>>();
    reverseDictionary = new HashMap<String, Map<Integer, String>>();
  }

  /**
   * Open datatable
   */
  public void open() {
    this.currentRowId = 0;
  }

  /**
   * Begin a new row
   */
  public void startRow() {
    isOpen = true;
    currentRowId = currentRowId + 1;
    currentRowData = ByteBuffer.allocate(rowSizeInBytes);
  }

  /**
   * set boolean column
   *
   * @param columnIndex
   * @param value
   */
  public void setColumn(int columnIndex, boolean value) {
    currentRowData.position(columnOffsets[columnIndex]);
    if (value) {
      currentRowData.put((byte) 1);
    } else {
      currentRowData.put((byte) 0);
    }
  }

  /**
   *
   * @param columnIndex
   * @param value
   */
  public void setColumn(int columnIndex, byte value) {
    currentRowData.position(columnOffsets[columnIndex]);
    currentRowData.put(value);
  }

  /**
   *
   * @param columnIndex
   * @param value
   */
  public void setColumn(int columnIndex, char value) {
    currentRowData.position(columnOffsets[columnIndex]);
    currentRowData.putChar(value);
  }

  /**
   *
   * @param columnIndex
   * @param value
   */
  public void setColumn(int columnIndex, short value) {
    currentRowData.position(columnOffsets[columnIndex]);
    currentRowData.putShort(value);
  }

  /**
   *
   * @param columnIndex
   * @param value
   */
  public void setColumn(int columnIndex, int value) {
    currentRowData.position(columnOffsets[columnIndex]);
    currentRowData.putInt(value);
  }

  /**
   *
   * @param columnIndex
   * @param value
   */
  public void setColumn(int columnIndex, long value) {
    currentRowData.position(columnOffsets[columnIndex]);
    currentRowData.putLong(value);
  }

  /**
   *
   * @param columnIndex
   * @param value
   */
  public void setColumn(int columnIndex, float value) {
    currentRowData.position(columnOffsets[columnIndex]);
    currentRowData.putFloat(value);
  }

  /**
   *
   * @param columnIndex
   * @param value
   */
  public void setColumn(int columnIndex, double value) {
    currentRowData.position(columnOffsets[columnIndex]);
    currentRowData.putDouble(value);
  }

  /**
   *
   * @param columnIndex
   * @param value
   * @throws Exception
   */
  public void setColumn(int columnIndex, String value) throws Exception {
    currentRowData.position(columnOffsets[columnIndex]);
    String columnName = schema.columnNames[columnIndex];
    if (dictionary.get(columnName) == null) {
      dictionary.put(columnName, new HashMap<String, Integer>());
      reverseDictionary.put(columnName, new HashMap<Integer, String>());

    }
    Map<String, Integer> map = dictionary.get(columnName);
    if (!map.containsKey(value)) {
      int id = map.size();
      map.put(value, id);
      reverseDictionary.get(columnName).put(id, value);
    }
    currentRowData.putInt(map.get(value));
  }

  /**
   *
   * @param columnIndex
   * @param value
   * @throws Exception
   */
  public void setColumn(int columnIndex, Object value) throws Exception {

    byte[] bytes = new byte[0];
    bytes = serializeObject(value);
    currentRowData.position(columnOffsets[columnIndex]);
    currentRowData.putInt(variableSizeDataHolder.position());
    variableSizeDataHolder.add(bytes);
    currentRowData.putInt(bytes.length);
  }

  // ARRAY TYPE support
  /**
   *
   * @param columnIndex
   * @param value
   * @throws Exception
   */
  public void setColumn(int columnIndex, byte[] value) throws Exception {
    currentRowData.position(columnOffsets[columnIndex]);
    currentRowData.putInt(variableSizeDataHolder.position());
    for (int i = 0; i < value.length; i++) {
      variableSizeDataHolder.add(value[i]);
    }
    currentRowData.putInt(value.length);
  }

  /**
   *
   * @param columnIndex
   * @param value
   * @throws Exception
   */
  public void setColumn(int columnIndex, char[] value) throws Exception {
    currentRowData.position(columnOffsets[columnIndex]);
    currentRowData.putInt(variableSizeDataHolder.position());
    for (int i = 0; i < value.length; i++) {
      variableSizeDataHolder.add(value[i]);
    }
    currentRowData.putInt(value.length);
  }

  /**
   *
   * @param columnIndex
   * @param value
   * @throws Exception
   */
  public void setColumn(int columnIndex, short[] value) throws Exception {
    currentRowData.position(columnOffsets[columnIndex]);
    currentRowData.putInt(variableSizeDataHolder.position());
    currentRowData.putInt(value.length);
    for (int i = 0; i < value.length; i++) {
      variableSizeDataHolder.add(value[i]);
    }
  }

  /**
   *
   * @param columnIndex
   * @param value
   * @throws Exception
   */
  public void setColumn(int columnIndex, int[] value) throws Exception {
    currentRowData.position(columnOffsets[columnIndex]);
    currentRowData.putInt(variableSizeDataHolder.position());
    currentRowData.putInt(value.length);
    for (int i = 0; i < value.length; i++) {
      variableSizeDataHolder.add(value[i]);
    }
  }

  /**
   *
   * @param columnIndex
   * @param value
   * @throws Exception
   */
  public void setColumn(int columnIndex, long[] value) throws Exception {
    currentRowData.position(columnOffsets[columnIndex]);
    currentRowData.putInt(variableSizeDataHolder.position());
    for (int i = 0; i < value.length; i++) {
      variableSizeDataHolder.add(value[i]);
    }
    currentRowData.putInt(value.length);
  }

  /**
   *
   * @param columnIndex
   * @param value
   * @throws Exception
   */
  public void setColumn(int columnIndex, float[] value) throws Exception {
    currentRowData.position(columnOffsets[columnIndex]);
    currentRowData.putInt(variableSizeDataHolder.position());
    for (int i = 0; i < value.length; i++) {
      variableSizeDataHolder.add(value[i]);
    }
    currentRowData.putInt(value.length);
  }

  /**
   *
   * @param columnIndex
   * @param value
   * @throws Exception
   */
  public void setColumn(int columnIndex, double[] value) throws Exception {
    currentRowData.position(columnOffsets[columnIndex]);
    currentRowData.putInt(variableSizeDataHolder.position());
    for (int i = 0; i < value.length; i++) {
      variableSizeDataHolder.add(value[i]);
    }
    currentRowData.putInt(value.length);
  }

  /**
   *
   * @param columnIndex
   * @param value
   * @throws Exception
   */
  public void setColumn(int columnIndex, String[] values) throws Exception {
    String columnName = schema.columnNames[columnIndex];
    if (dictionary.get(columnName) == null) {
      dictionary.put(columnName, new HashMap<String, Integer>());
      reverseDictionary.put(columnName, new HashMap<Integer, String>());

    }
    Map<String, Integer> map = dictionary.get(columnName);
    for (String value : values) {
      if (!map.containsKey(value)) {
        int id = map.size();
        map.put(value, id);
        reverseDictionary.get(columnName).put(id, value);
      }
    }
    currentRowData.position(columnOffsets[columnIndex]);
    currentRowData.putInt(variableSizeDataHolder.position());
    for (int i = 0; i < values.length; i++) {
      variableSizeDataHolder.add(map.get(values[i]));
    }
    currentRowData.putInt(values.length);
  }

  /**
   *
   * @param value
   * @return
   */
  private byte[] serializeObject(Object value) {
    byte[] bytes;
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutput out = null;

    try {
      try {
        out = new ObjectOutputStream(bos);
        out.writeObject(value);
      } catch (IOException e) {
        // TODO: log exception
      }
      bytes = bos.toByteArray();
    } finally {
      try {
        if (out != null) {
          out.close();
        }
      } catch (IOException ex) {
        // ignore close exception
      }
      try {
        bos.close();
      } catch (IOException ex) {
        // ignore close exception
      }
    }
    return bytes;
  }

  /**
   *
   * @throws Exception
   */
  public void finishRow() throws Exception {
    fixedSizeDataHolder.add(currentRowData.array());
  }

  /**
   *
   * @param key
   * @param value
   */
  public void addMetaData(String key, String value) {
    metadata.put(key, value);
  }

  /**
  *
  */
  public void seal() {
    isOpen = false;
  }

  /**
   *
   * @return
   * @throws Exception
   */
  public DataTable build() throws Exception {

    return new DataTable(currentRowId, reverseDictionary, metadata, schema, fixedSizeDataHolder.toBytes(),
        variableSizeDataHolder.toBytes());
  }

  /**
   *
   * @return
   */
  public DataTable buildExceptions() {
    return new DataTable(metadata);
  }

  /**
   *
   * Simple class to describe the schema of DataTable
   */
  public static class DataSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    public DataSchema(String[] columnNames, DataType[] columnTypes) {
      this.columnNames = columnNames;
      this.columnTypes = columnTypes;
    }

    String[] columnNames;
    DataType[] columnTypes;

    public int size() {
      return columnNames.length;
    }

    public String getColumnName(int idx) {
      return columnNames[idx];
    }

    public DataType getColumnType(int idx) {
      return columnTypes[idx];
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      String isMultiValue;
      String delim = "[";
      for (int i = 0; i < size(); ++i) {
        if (columnTypes[i].isSingleValue()) {
          isMultiValue = "Single Value";
        } else {
          isMultiValue = "Multi Value";
        }
        sb.append(delim + columnNames[i] + "(" + columnTypes[i] + ", " + isMultiValue + ")");
        delim = ",";
      }
      sb.append("]");
      return sb.toString();
    }
  }

  /**
   * Generic class to hold bytes. A simple wrapper around data output stream
   *
   */
  class ByteHolder {

    int currentPosition = 0;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream data = new DataOutputStream(baos);

    public int position() {
      return currentPosition;
    }

    public void add(byte b) throws IOException {
      this.data.writeByte(b);
      currentPosition = currentPosition + (Byte.SIZE >> 3);
    }

    public void add(char c) throws IOException {
      this.data.writeChar(c);
      currentPosition = currentPosition + (Character.SIZE >> 3);
    }

    public void add(int i) throws IOException {
      this.data.writeInt(i);
      currentPosition = currentPosition + (Integer.SIZE >> 3);
    }

    public void add(long l) throws IOException {
      this.data.writeLong(l);
      currentPosition = currentPosition + (Long.SIZE >> 3);
    }

    public void add(float f) throws IOException {
      this.data.writeFloat(f);
      currentPosition = currentPosition + (Float.SIZE >> 3);
    }

    public void add(double d) throws IOException {
      this.data.writeDouble(d);
      currentPosition = currentPosition + (Double.SIZE >> 3);
    }

    public void add(byte[] data) throws Exception {
      this.data.write(data);
      currentPosition = currentPosition + data.length;
    }

    public int size() {
      return data.size();
    }

    public byte[] toBytes() throws IOException {
      baos.flush();
      return baos.toByteArray();
    }
  }
}
