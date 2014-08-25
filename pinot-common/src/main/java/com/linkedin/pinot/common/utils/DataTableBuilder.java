package com.linkedin.pinot.common.utils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
		columnOffsets = new int[schema.columnNames.length];
		fixedSizeDataHolder = new ByteHolder();
		variableSizeDataHolder = new ByteHolder();
		for (int i = 0; i < schema.columnNames.length; i++) {
			DataType type = schema.columnTypes[i];
			columnOffsets[i] = rowSizeInBytes;
			switch (type) {
			case CHAR:
				rowSizeInBytes += 2;
				break;
			case BYTE:
				rowSizeInBytes += 1;
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
		dictionary = new HashMap<String, Map<String, Integer>>();
		reverseDictionary = new HashMap<String, Map<Integer, String>>();
	}

	public void open() {
		this.currentRowId = 0;
	}

	public void startRow() {
		isOpen = true;
		currentRowId = currentRowId + 1;
		currentRowData = ByteBuffer.allocate(rowSizeInBytes);
	}

	public void setColumn(int columnIndex, byte value) {
		currentRowData.position(columnOffsets[columnIndex]);
		currentRowData.put(value);
	}

	public void setColumn(int columnIndex, char value) {
		currentRowData.position(columnOffsets[columnIndex]);
		currentRowData.putChar(value);
	}

	public void setColumn(int columnIndex, short value) {
		currentRowData.position(columnOffsets[columnIndex]);
		currentRowData.putShort(value);
	}

	public void setColumn(int columnIndex, int value) {
		currentRowData.position(columnOffsets[columnIndex]);
		currentRowData.putInt(value);
	}

	public void setColumn(int columnIndex, long value) {
		currentRowData.position(columnOffsets[columnIndex]);
		currentRowData.putLong(value);
	}

	public void setColumn(int columnIndex, float value) {
		currentRowData.position(columnOffsets[columnIndex]);
		currentRowData.putFloat(value);
	}

	public void setColumn(int columnIndex, double value) {
		currentRowData.position(columnOffsets[columnIndex]);
		currentRowData.putDouble(value);
	}

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

	public void setColumn(int columnIndex, Object value) throws Exception {

		byte[] bytes = new byte[0];
		bytes = serializeObject(value);
		currentRowData.position(columnOffsets[columnIndex]);
		currentRowData.putInt(variableSizeDataHolder.position());
		variableSizeDataHolder.add(bytes);
		currentRowData.putInt(bytes.length);
	}

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

	public void finishRow() throws Exception {
		fixedSizeDataHolder.add(currentRowData.array());

	}

	public void addMetaData(String key, String value) {
		metadata.put(key, value);
	}

	public void seal() {
		isOpen = false;
	}

	public DataTable build() throws Exception {

		return new DataTable(currentRowId, reverseDictionary, metadata, schema,
				fixedSizeDataHolder.toBytes(), variableSizeDataHolder.toBytes());
	}

	public static class DataSchema implements Serializable {

		public DataSchema(String[] columnNames, DataType[] columnTypes) {
			this.columnNames = columnNames;
			this.columnTypes = columnTypes;
		}

		String[] columnNames;

		DataType[] columnTypes;
	}

	public enum DataType {
		CHAR, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, STRING, OBJECT
	}

	class ByteHolder {

		int currentPosition = 0;
		ByteArrayOutputStream data = new ByteArrayOutputStream();

		public int position() {
			return currentPosition;
		}

		public void add(byte[] data) throws Exception {
			this.data.write(data);
			currentPosition = currentPosition + data.length;
		}

		public int size() {
			return data.size();
		}

		public byte[] toBytes() {
			return data.toByteArray();
		}
	}

	class CustomKeyValueHolder {

	}

}
