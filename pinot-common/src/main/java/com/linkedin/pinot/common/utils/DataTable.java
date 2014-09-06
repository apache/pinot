package com.linkedin.pinot.common.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.utils.DataTableBuilder.DataSchema;


public class DataTable {

  int numRows;

  int numCols;

  DataSchema schema;

  static int VERSION = 1;

  private Map<String, Map<Integer, String>> dictionary;

  private Map<String, String> metadata;

  private ByteBuffer fixedSizeData;

  private ByteBuffer variableSizeData;

  private int[] columnOffsets;

  private int rowSizeInBytes;

  private byte[] fixedSizeDataBytes;

  private byte[] variableSizeDataBytes;

  public DataTable(int numRows, Map<String, Map<Integer, String>> dictionary, Map<String, String> metadata,
      DataSchema schema, byte[] fixedSizeDataBytes, byte[] variableSizeDataBytes) throws Exception {
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

  public DataTable(Map<String, String> metadata) {
    this.metadata = metadata;
  }

  private int[] computeColumnOffsets(DataSchema schema) {
    final int[] columnOffsets = new int[schema.columnNames.length];
    for (int i = 0; i < schema.columnNames.length; i++) {
      final DataType type = schema.columnTypes[i];
      columnOffsets[i] = rowSizeInBytes;
      switch (type) {
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
        default:
          break;
      }
    }
    return columnOffsets;
  }

  public DataTable(byte[] buffer) {

    final ByteBuffer input = ByteBuffer.wrap(buffer);

    final int version = input.getInt();
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
    final byte[] dictionaryBytes = new byte[dictionaryLength];
    input.position(dictionaryStart);
    input.get(dictionaryBytes);
    dictionary = deserialize(dictionaryBytes);

    // READ METADATA
    final byte[] metadataBytes = new byte[metadataLength];
    input.position(metadataStart);
    input.get(metadataBytes);
    metadata = deserialize(metadataBytes);

    // READ METADATA
    final byte[] schemaBytes = new byte[schemaLength];
    input.position(schemaStart);
    input.get(schemaBytes);
    schema = deserialize(schemaBytes);
    columnOffsets = computeColumnOffsets(schema);

    // READ METADATA
    fixedSizeDataBytes = new byte[fixedDataLength];
    input.position(fixedDataStart);
    input.get(fixedSizeDataBytes);
    fixedSizeData = ByteBuffer.wrap(fixedSizeDataBytes);

    variableSizeDataBytes = new byte[variableDataLength];
    input.position(variableDataStart);
    input.get(variableSizeDataBytes);
    variableSizeData = ByteBuffer.wrap(variableSizeDataBytes);

  }

  public DataTable() {
    // Used for empty results.
  }

  public byte[] toBytes() throws Exception {
    final byte[] dictionaryBytes = serializeObject(dictionary);
    final byte[] metadataBytes = serializeObject(metadata);
    final byte[] schemaBytes = serializeObject(schema);
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final DataOutputStream out = new DataOutputStream(baos);
    // TODO: convert this format into a proper class
    // VERSION|NUM_ROW|NUM_COL|(START|SIZE) -- START|SIZE 5 PAIRS FOR
    // DICTIONARY, METADATA,
    // SCHEMA, DATATABLE, VARIABLE DATA BUFFER --> 4 + 4 + 4 + 5*8 = 52
    // bytes
    out.writeInt(VERSION);
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
    out.writeInt(fixedSizeDataBytes.length);
    baseOffset += fixedSizeDataBytes.length;

    // variable data
    out.writeInt(baseOffset);
    out.writeInt(variableSizeDataBytes.length);

    // write them
    out.write(dictionaryBytes);
    out.write(metadataBytes);
    out.write(schemaBytes);
    out.write(fixedSizeDataBytes);
    out.write(variableSizeDataBytes);
    return baos.toByteArray();
  }

  private byte[] serializeObject(Object value) {
    byte[] bytes;
    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutput out = null;

    try {
      try {
        out = new ObjectOutputStream(bos);
        out.writeObject(value);
      } catch (final IOException e) {
        // TODO: log exception
      }
      bytes = bos.toByteArray();
    } finally {
      try {
        if (out != null) {
          out.close();
        }
      } catch (final IOException ex) {
        // ignore close exception
      }
      try {
        bos.close();
      } catch (final IOException ex) {
        // ignore close exception
      }
    }
    return bytes;
  }

  @SuppressWarnings("unchecked")
  private <T extends Serializable> T deserialize(byte[] value) {
    final ByteArrayInputStream bais = new ByteArrayInputStream(value);
    ObjectInput out = null;

    try {
      try {
        out = new ObjectInputStream(bais);
        final Object readObject = out.readObject();
        return (T) readObject;
      } catch (final Exception e) {
        e.printStackTrace();
        return null;
      }
    } finally {
      try {
        if (out != null) {
          out.close();
        }
      } catch (final IOException ex) {
        // ignore close exception
      }
      try {
        bais.close();
      } catch (final IOException ex) {
        // ignore close exception
      }
    }
  }

  public int getNumberOfRows() {
    return numRows;
  }

  public int getNumberOfCols() {
    return numCols;
  }

  public DataSchema getDataSchema() {
    return schema;
  }

  public char getChar(int rowId, int colId) {
    fixedSizeData.position(rowId * rowSizeInBytes + columnOffsets[colId]);
    return fixedSizeData.getChar();
  }

  public byte getByte(int rowId, int colId) {
    fixedSizeData.position(rowId * rowSizeInBytes + columnOffsets[colId]);
    return fixedSizeData.get();
  }

  public short getShort(int rowId, int colId) {
    fixedSizeData.position(rowId * rowSizeInBytes + columnOffsets[colId]);
    return fixedSizeData.getShort();
  }

  public int getInt(int rowId, int colId) {
    fixedSizeData.position(rowId * rowSizeInBytes + columnOffsets[colId]);
    return fixedSizeData.getInt();
  }

  public long getLong(int rowId, int colId) {
    fixedSizeData.position(rowId * rowSizeInBytes + columnOffsets[colId]);
    return fixedSizeData.getLong();
  }

  public float getFloat(int rowId, int colId) {
    fixedSizeData.position(rowId * rowSizeInBytes + columnOffsets[colId]);
    return fixedSizeData.getFloat();
  }

  public double getDouble(int rowId, int colId) {
    fixedSizeData.position(rowId * rowSizeInBytes + columnOffsets[colId]);
    return fixedSizeData.getDouble();
  }

  public String getString(int rowId, int colId) {
    fixedSizeData.position(rowId * rowSizeInBytes + columnOffsets[colId]);
    final int id = fixedSizeData.getInt();
    final Map<Integer, String> map = dictionary.get(schema.columnNames[colId]);
    return map.get(id);
  }

  @SuppressWarnings("unchecked")
  public <T extends Serializable> T getObject(int rowId, int colId) {
    fixedSizeData.position(rowId * rowSizeInBytes + columnOffsets[colId]);
    // find the position and length in the variabledata
    final int position = fixedSizeData.getInt();
    final int length = fixedSizeData.getInt();
    variableSizeData.position(position);
    final byte[] serData = new byte[length];
    variableSizeData.get(serData);
    return (T) deserialize(serData);
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder();
    b.append(schema.toString());
    b.append("\n");

    b.append("numRows : " + numRows);
    return b.toString();
  }
}
