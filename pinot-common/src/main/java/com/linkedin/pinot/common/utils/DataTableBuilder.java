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

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * Datatable that holds data in a matrix form. The purpose of this class is to
 * provide a way to construct a datatable and ability to serialize and
 * deserialize.<br>
 * Why can't we use existing serialization/deserialization mechanism. Most
 * existing techniques protocol buffer, thrift, avro are optimized for
 * transporting a single record but Pinot transfers quite a lot of data from
 * server to broker during the scatter/gather operation. The cost of
 * serialization and deserialization directly impacts the performance. Most
 * ser/deser requires us to convert the primitives data types in objects like
 * Integer etc. This is waste of cpu resource and increase the payload size. We
 * optimize the data format for Pinot usecase. We can also support lazy
 * construction of obejcts. Infact we retain the bytes as it is and will be able
 * to lookup the a field directly within a byte buffer.<br>
 *
 * USAGE:
 *
 * Datatable is initialized with the schema of the table. Schema describes the
 * columnnames, their order and data type for each column.<br>
 * Each row must follow the same convention. We don't support MultiValue columns
 * for now. Format,
 * |VERSION,DATA_START_OFFSET,DICTIONARY_START_OFFSET,INDEX_START_OFFSET
 * ,METADATA_START_OFFSET | |&lt;DATA&gt; |
 *
 * |&lt;DICTIONARY&gt;|
 *
 *
 * |&lt;METADATA&gt;| Data contains the actual values written by the application We
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
 *
 */
public class DataTableBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataTableBuilder.class);

  /**
   * Initialize the datatable with metadata
   */

  // Serializer/De-serializer for the DataTable.
  private  DataTableSerDe dataTableSerDe;

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
   * format length of header. VERSION, <br>
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
  private DataTable.Version version;

  public DataTableBuilder(DataSchema schema) {
    this.dataTableSerDe = DataTableSerDeRegistry.getInstance().get();

    // Derive the version from the ser/de that is registered.
    this.version = DataTable.deriveVersionFromDataTableSerDe(dataTableSerDe);

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
    byte[] bytes = dataTableSerDe.serialize(value);
    currentRowData.position(columnOffsets[columnIndex]);
    currentRowData.putInt(variableSizeDataHolder.position());

    // For custom serialization, we need to write the object type as well.
    if (version == DataTable.Version.V2) {
      variableSizeDataHolder.add(dataTableSerDe.getObjectType(value).getValue());
    }

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
   * @param values
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
    return new DataTable(version, currentRowId, reverseDictionary, metadata, schema, fixedSizeDataHolder.toBytes(),
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

    /**
     * Indicates whether the given {@link DataSchema} is type compatible with this one.
     * <ul>
     *   <li>All numbers are type compatible.</li>
     *   <li>Number is not type compatible with String.</li>
     *   <li>Single-value is not type compatible with multi-value.</li>
     * </ul>
     *
     * @param anotherDataSchema data schema to compare.
     * @return whether the two data schemas are type compatible.
     */
    public boolean isTypeCompatibleWith(@Nonnull DataSchema anotherDataSchema) {
      if (!Arrays.equals(columnNames, anotherDataSchema.columnNames)) {
        return false;
      }
      int numColumns = columnNames.length;
      for (int i = 0; i < numColumns; i++) {
        if (!columnTypes[i].isCompatible(anotherDataSchema.columnTypes[i])) {
          return false;
        }
      }
      return true;
    }

    /**
     * Upgrade the current data schema to cover the column types in the given data schema.
     * <p>Type <code>long</code> can cover <code>int</code> and <code>long</code>.
     * <p>Type <code>double</code> can cover all numbers, but with potential precision loss when use it to cover
     * <code>long</code>.
     * <p>The given data schema should be type compatible with this one.
     *
     * @param anotherDataSchema data schema to be covered.
     */
    public void upgradeToCover(@Nonnull DataSchema anotherDataSchema) {
      int numColumns = columnTypes.length;
      for (int i = 0; i < numColumns; i++) {
        DataType thisColumnType = columnTypes[i];
        DataType thatColumnType = anotherDataSchema.columnTypes[i];
        if (thisColumnType != thatColumnType) {
          if (thisColumnType.isSingleValue()) {
            if (thisColumnType.isInteger() && thatColumnType.isInteger()) {
              columnTypes[i] = DataType.LONG;
            } else {
              columnTypes[i] = DataType.DOUBLE;
            }
          } else {
            if (thisColumnType.toSingleValue().isInteger() && thatColumnType.toSingleValue().isInteger()) {
              columnTypes[i] = DataType.LONG_ARRAY;
            } else {
              columnTypes[i] = DataType.DOUBLE_ARRAY;
            }
          }
        }
      }
    }

    public byte[] toBytes() throws Exception {
      if (columnNames == null || columnNames.length == 0) {
        return new byte[0];
      }
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(out);
      int length = columnNames.length;
      //write the number of fields
      dos.writeInt(length);
      //write the columnNames
      for (int i = 0; i < length; i++) {
        byte[] bytes = columnNames[i].getBytes();
        dos.writeInt(bytes.length);
        dos.write(bytes);
      }
      // Write the DataTypes
      for (int i = 0; i < length; i++) {
        // We don't want to use ordinal of the enum (even though its reduces the data size)
        // since adding a new data type will break things if server and broker use different versions of DataType class.
        byte[] bytes = columnTypes[i].name().getBytes();
        dos.writeInt(bytes.length);
        dos.write(bytes);
      }
      return out.toByteArray();
    }

    public static DataSchema fromBytes(byte[] buffer) {
      if (buffer == null || buffer.length == 0) {
        return null;
      }
      ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
      DataInputStream dis = new DataInputStream(bais);
      try {

        int length;
        length = dis.readInt();
        String[] columnNames = new String[length];
        DataType[] columnTypes = new DataType[length];

        for (int i = 0; i < length; i++) {
          int size = dis.readInt();
          byte[] bytes = new byte[size];
          dis.read(bytes);
          columnNames[i] = new String(bytes);
        }
        for (int i = 0; i < length; i++) {
          int size = dis.readInt();
          byte[] bytes = new byte[size];
          dis.read(bytes);
          columnTypes[i] = DataType.valueOf(new String(bytes));
        }
        DataSchema schema = new DataSchema(columnNames, columnTypes);
        return schema;

      } catch (IOException e) {
        LOGGER.error("Exception deserializing DataSchema", e);
        return new DataSchema(new String[] {}, new DataType[] {});
      }
    }

    @SuppressWarnings("CloneDoesntCallSuperClone")
    @Override
    public DataSchema clone() {
      return new DataSchema(columnNames.clone(), columnTypes.clone());
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

    @Override
    public boolean equals(Object right) {
      if (EqualityUtils.isSameReference(this, right)) {
        return true;
      }

      if (EqualityUtils.isNullOrNotSameClass(this, right)) {
        return false;
      }

      DataSchema that = (DataSchema) right;

      return EqualityUtils.isEqual(this.columnNames, that.columnNames) &&
          EqualityUtils.isEqual(this.columnTypes, that.columnTypes);
    }

    @Override
    public int hashCode() {
      int hashCode = EqualityUtils.hashCodeOf(columnNames);
      hashCode = EqualityUtils.hashCodeOf(hashCode, columnTypes);
      return hashCode;
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

  /**
   * Helper method to set the data table ser/de, only used for
   * testing backward compatibility. DataTable ser/de should always be
   * queried from {@link DataTableSerDeRegistry}
   *
   * @param dataTableSerDe Ser/De to set.
   */
  @VisibleForTesting
  protected void setDataTableSerDe(DataTableSerDe dataTableSerDe) {
    this.dataTableSerDe = dataTableSerDe;
    this.version = DataTable.deriveVersionFromDataTableSerDe(dataTableSerDe);
  }
}
