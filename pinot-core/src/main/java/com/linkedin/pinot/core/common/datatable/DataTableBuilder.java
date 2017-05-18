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
package com.linkedin.pinot.core.common.datatable;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.utils.DataSchema;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionContext;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunction;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;


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
// TODO: potential optimizations:
// TODO:   1. Fix float size.
// TODO:   2. Use one dictionary for all columns (save space).
// TODO:   3. Given a data schema, write all values one by one instead of using rowId and colId to position (save time).
public class DataTableBuilder {
  private static final String COUNT_STAR = "count_star";
  private final DataSchema _dataSchema;
  private final int[] _columnOffsets;
  private final int _rowSizeInBytes;
  private final Map<String, Map<String, Integer>> _dictionaryMap = new HashMap<>();
  private final Map<String, Map<Integer, String>> _reverseDictionaryMap = new HashMap<>();
  private final ByteArrayOutputStream _fixedSizeDataByteArrayOutputStream = new ByteArrayOutputStream();
  private final ByteArrayOutputStream _variableSizeDataByteArrayOutputStream = new ByteArrayOutputStream();
  private final DataOutputStream _variableSizeDataOutputStream =
      new DataOutputStream(_variableSizeDataByteArrayOutputStream);

  private int _numRows;
  private ByteBuffer _currentRowDataByteBuffer;

  public DataTableBuilder(@Nonnull DataSchema dataSchema) {
    _dataSchema = dataSchema;
    _columnOffsets = new int[dataSchema.size()];
    _rowSizeInBytes = DataTableUtils.computeColumnOffsets(dataSchema, _columnOffsets);
  }

  public void startRow() {
    _numRows++;
    _currentRowDataByteBuffer = ByteBuffer.allocate(_rowSizeInBytes);
  }

  public void setColumn(int colId, boolean value) {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    if (value) {
      _currentRowDataByteBuffer.put((byte) 1);
    } else {
      _currentRowDataByteBuffer.put((byte) 0);
    }
  }

  public void setColumn(int colId, byte value) {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.put(value);
  }

  public void setColumn(int colId, char value) {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putChar(value);
  }

  public void setColumn(int colId, short value) {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putShort(value);
  }

  public void setColumn(int colId, int value) {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(value);
  }

  public void setColumn(int colId, long value) {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putLong(value);
  }

  public void setColumn(int colId, float value) {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putFloat(value);
  }

  public void setColumn(int colId, double value) {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putDouble(value);
  }

  public void setColumn(int colId, @Nonnull String value) {
    String columnName = _dataSchema.getColumnName(colId);
    Map<String, Integer> dictionary = _dictionaryMap.get(columnName);
    if (dictionary == null) {
      dictionary = new HashMap<>();
      _dictionaryMap.put(columnName, dictionary);
      _reverseDictionaryMap.put(columnName, new HashMap<Integer, String>());
    }

    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    Integer dictId = dictionary.get(value);
    if (dictId == null) {
      dictId = dictionary.size();
      dictionary.put(value, dictId);
      _reverseDictionaryMap.get(columnName).put(dictId, value);
    }
    _currentRowDataByteBuffer.putInt(dictId);
  }

  public void setColumn(int colId, @Nonnull Object value)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    byte[] bytes = ObjectCustomSerDe.serialize(value);
    _currentRowDataByteBuffer.putInt(bytes.length);
    _variableSizeDataOutputStream.writeInt(ObjectCustomSerDe.getObjectType(value).getValue());
    _variableSizeDataByteArrayOutputStream.write(bytes);
  }

  public void setColumn(int colId, @Nonnull byte[] values) {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    _currentRowDataByteBuffer.putInt(values.length);
    for (byte value : values) {
      _variableSizeDataByteArrayOutputStream.write(value);
    }
  }

  public void setColumn(int colId, @Nonnull char[] values)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    _currentRowDataByteBuffer.putInt(values.length);
    for (char value : values) {
      _variableSizeDataOutputStream.writeChar(value);
    }
  }

  public void setColumn(int colId, @Nonnull short[] values)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    _currentRowDataByteBuffer.putInt(values.length);
    for (short value : values) {
      _variableSizeDataOutputStream.writeShort(value);
    }
  }

  public void setColumn(int colId, @Nonnull int[] values)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    _currentRowDataByteBuffer.putInt(values.length);
    for (int value : values) {
      _variableSizeDataOutputStream.writeInt(value);
    }
  }

  public void setColumn(int colId, @Nonnull long[] values)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    _currentRowDataByteBuffer.putInt(values.length);
    for (long value : values) {
      _variableSizeDataOutputStream.writeLong(value);
    }
  }

  public void setColumn(int colId, @Nonnull float[] values)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    _currentRowDataByteBuffer.putInt(values.length);
    for (float value : values) {
      _variableSizeDataOutputStream.writeFloat(value);
    }
  }

  public void setColumn(int colId, @Nonnull double[] values)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    _currentRowDataByteBuffer.putInt(values.length);
    for (double value : values) {
      _variableSizeDataOutputStream.writeDouble(value);
    }
  }

  public void setColumn(int colId, @Nonnull String[] values)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    _currentRowDataByteBuffer.putInt(values.length);

    String columnName = _dataSchema.getColumnName(colId);
    Map<String, Integer> dictionary = _dictionaryMap.get(columnName);
    if (dictionary == null) {
      dictionary = new HashMap<>();
      _dictionaryMap.put(columnName, dictionary);
      _reverseDictionaryMap.put(columnName, new HashMap<Integer, String>());
    }

    for (String value : values) {
      Integer dictId = dictionary.get(value);
      if (dictId == null) {
        dictId = dictionary.size();
        dictionary.put(value, dictId);
        _reverseDictionaryMap.get(columnName).put(dictId, value);
      }
      _variableSizeDataOutputStream.writeInt(dictId);
    }
  }

  public void finishRow()
      throws IOException {
    _fixedSizeDataByteArrayOutputStream.write(_currentRowDataByteBuffer.array());
  }

  public DataTable build() {
    return new DataTableImplV2(_numRows, _dataSchema, _reverseDictionaryMap,
        _fixedSizeDataByteArrayOutputStream.toByteArray(), _variableSizeDataByteArrayOutputStream.toByteArray());
  }

  /**
   * Build an empty data table based on the broker request.
   */
  public static DataTable buildEmptyDataTable(BrokerRequest brokerRequest)
      throws IOException {
    // Selection query.
    if (brokerRequest.isSetSelections()) {
      Selection selection = brokerRequest.getSelections();
      List<String> selectionColumns = selection.getSelectionColumns();
      int numSelectionColumns = selectionColumns.size();
      FieldSpec.DataType[] dataTypes = new FieldSpec.DataType[numSelectionColumns];
      // Use STRING data type as default for selection query.
      Arrays.fill(dataTypes, FieldSpec.DataType.STRING);
      DataSchema dataSchema = new DataSchema(selectionColumns.toArray(new String[numSelectionColumns]), dataTypes);
      return new DataTableBuilder(dataSchema).build();
    }

    // Aggregation query.
    List<AggregationInfo> aggregationsInfo = brokerRequest.getAggregationsInfo();
    int numAggregations = aggregationsInfo.size();
    AggregationFunctionContext[] aggregationFunctionContexts = new AggregationFunctionContext[numAggregations];
    for (int i = 0; i < numAggregations; i++) {
      aggregationFunctionContexts[i] = AggregationFunctionContext.instantiate(aggregationsInfo.get(i));
    }
    if (brokerRequest.isSetGroupBy()) {
      // Aggregation group-by query.

      String[] columnNames = new String[]{"functionName", "GroupByResultMap"};
      FieldSpec.DataType[] columnTypes = new FieldSpec.DataType[]{FieldSpec.DataType.STRING, FieldSpec.DataType.OBJECT};

      // Build the data table.
      DataTableBuilder dataTableBuilder = new DataTableBuilder(new DataSchema(columnNames, columnTypes));
      for (int i = 0; i < numAggregations; i++) {
        dataTableBuilder.startRow();
        dataTableBuilder.setColumn(0, aggregationFunctionContexts[i].getAggregationColumnName());
        dataTableBuilder.setColumn(1, new HashMap<String, Object>());
        dataTableBuilder.finishRow();
      }
      return dataTableBuilder.build();
    } else {
      // Aggregation only query.

      String[] aggregationColumnNames = new String[numAggregations];
      FieldSpec.DataType[] dataTypes = new FieldSpec.DataType[numAggregations];
      Object[] aggregationResults = new Object[numAggregations];
      for (int i = 0; i < numAggregations; i++) {
        AggregationFunctionContext aggregationFunctionContext = aggregationFunctionContexts[i];
        aggregationColumnNames[i] = aggregationFunctionContext.getAggregationColumnName();
        AggregationFunction aggregationFunction = aggregationFunctionContext.getAggregationFunction();
        dataTypes[i] = aggregationFunction.getIntermediateResultDataType();
        aggregationResults[i] =
            aggregationFunction.extractAggregationResult(aggregationFunction.createAggregationResultHolder());
      }

      // Build the data table.
      DataTableBuilder dataTableBuilder = new DataTableBuilder(new DataSchema(aggregationColumnNames, dataTypes));
      dataTableBuilder.startRow();
      for (int i = 0; i < numAggregations; i++) {
        switch (dataTypes[i]) {
          case LONG:
            dataTableBuilder.setColumn(i, ((Number) aggregationResults[i]).longValue());
            break;
          case DOUBLE:
            dataTableBuilder.setColumn(i, ((Double) aggregationResults[i]).doubleValue());
            break;
          case OBJECT:
            dataTableBuilder.setColumn(i, aggregationResults[i]);
            break;
          default:
            throw new UnsupportedOperationException(
                "Unsupported aggregation column data type: " + dataTypes[i] + " for column: "
                    + aggregationColumnNames[i]);
        }
      }
      dataTableBuilder.finishRow();
      return dataTableBuilder.build();
    }
  }

  /**
   * Builds a Data table for count(*) with the provided value.
   *
   * @param count Value of count(*)
   * @return DataTable for count(*)
   *
   * @throws IOException
   */
  public static DataTable buildCountStarDataTable(long count)
      throws IOException {
    String[] aggregationColumnNames = new String[]{COUNT_STAR};
    FieldSpec.DataType[] dataTypes = new FieldSpec.DataType[] {FieldSpec.DataType.LONG};

    DataTableBuilder dataTableBuilder = new DataTableBuilder(new DataSchema(aggregationColumnNames, dataTypes));
    dataTableBuilder.startRow();
    dataTableBuilder.setColumn(0, count);
    dataTableBuilder.finishRow();
    DataTable dataTable = dataTableBuilder.build();
    Map<String, String> metadata = dataTable.getMetadata();

    // Set num docs scanned as well, for backward compatibility.
    String totalDocs = Long.toString(count);
    metadata.put(DataTable.NUM_DOCS_SCANNED_METADATA_KEY, totalDocs);
    metadata.put(DataTable.TOTAL_DOCS_METADATA_KEY, totalDocs);
    return dataTable;
  }
}
