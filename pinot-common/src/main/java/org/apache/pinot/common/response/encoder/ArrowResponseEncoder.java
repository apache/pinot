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
package org.apache.pinot.common.response.encoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.utils.MapUtils;


public class ArrowResponseEncoder implements ResponseEncoder {
  private static final RootAllocator ALLOCATOR = new RootAllocator(Long.MAX_VALUE);

  @Override
  public byte[] encodeResultTable(ResultTable resultTable, int startRow, int length)
      throws IOException {
    // Create an Arrow VectorSchemaRoot from a subrange of rows.
    try (VectorSchemaRoot root = createVectorSchemaRoot(resultTable, resultTable.getDataSchema(), startRow, length);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
      writer.start();
      writer.writeBatch();
      writer.end();
      return out.toByteArray();
    }
  }

  private VectorSchemaRoot createVectorSchemaRoot(ResultTable resultTable, DataSchema schema, int startRow,
      int length) {
    List<Field> fields = new ArrayList<>();
    List<FieldVector> vectors = new ArrayList<>();
    int numColumns = schema.getColumnNames().length; // assuming getter returns String[]

    // Create an Arrow Field and vector for each column based on its type.
    for (int col = 0; col < numColumns; col++) {
      String colName = schema.getColumnNames()[col];
      DataSchema.ColumnDataType colType = schema.getColumnDataTypes()[col];
      Field field;
      FieldVector vector;
      switch (colType) {
        case BOOLEAN:
          field = new Field(colName, FieldType.nullable(new ArrowType.Bool()), null);
          vector = new org.apache.arrow.vector.BitVector(colName, ALLOCATOR);
          break;
        case INT:
          field = new Field(colName, FieldType.nullable(new ArrowType.Int(32, true)), null);
          vector = new IntVector(colName, ALLOCATOR);
          break;
        case LONG:
          field = new Field(colName, FieldType.nullable(new ArrowType.Int(64, true)), null);
          vector = new BigIntVector(colName, ALLOCATOR);
          break;
        case FLOAT:
          field =
              new Field(colName, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);
          vector = new Float4Vector(colName, ALLOCATOR);
          break;
        case DOUBLE:
          field =
              new Field(colName, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
          vector = new Float8Vector(colName, ALLOCATOR);
          break;
        case TIMESTAMP:
        case STRING:
        case BYTES:
        case BIG_DECIMAL:
        case JSON:
        case OBJECT:
          field = new Field(colName, FieldType.nullable(new ArrowType.Utf8()), null);
          vector = new VarCharVector(colName, ALLOCATOR);
          break;
        case MAP:
          field = new Field(colName, FieldType.nullable(new ArrowType.Binary()), null);
          vector = new VarBinaryVector(colName, ALLOCATOR);
          break;
        case UNKNOWN:
          // handle Null value
          field = new Field(colName, FieldType.nullable(new ArrowType.Null()), null);
          vector = new NullVector(colName);
          break;
        case BOOLEAN_ARRAY:
          // Define the inner field for a boolean element.
          List<Field> children =
              Collections.singletonList(new Field("element", FieldType.nullable(new ArrowType.Bool()), null));
          // Define the field for the list column.
          field = new Field(colName, FieldType.nullable(new ArrowType.List()), children);
          // Create a ListVector using the field name and allocator.
          vector = ListVector.empty(colName, ALLOCATOR);
          // Initialize its children from the defined field.
          vector.initializeChildrenFromFields(children);
          break;
        case INT_ARRAY:
          // Define the inner field for an int element.
          children =
              Collections.singletonList(new Field("element", FieldType.nullable(new ArrowType.Int(32, true)), null));
          // Define the field for the list column.
          field = new Field(colName, FieldType.nullable(new ArrowType.List()), children);
          // Create a ListVector using the field name and allocator.
          vector = ListVector.empty(colName, ALLOCATOR);
          // Initialize its children from the defined field.
          vector.initializeChildrenFromFields(children);
          break;
        case LONG_ARRAY:
          // Define the inner field for a long element.
          children =
              Collections.singletonList(new Field("element", FieldType.nullable(new ArrowType.Int(64, true)), null));
          // Define the field for the list column.
          field = new Field(colName, FieldType.nullable(new ArrowType.List()), children);
          // Create a ListVector using the field name and allocator.
          vector = ListVector.empty(colName, ALLOCATOR);
          // Initialize its children from the defined field.
          vector.initializeChildrenFromFields(children);
          break;
        case FLOAT_ARRAY:
          // Define the inner field for a float element.
          children = Collections.singletonList(
              new Field("element", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                  null));
          // Define the field for the list column.
          field = new Field(colName, FieldType.nullable(new ArrowType.List()), children);
          // Create a ListVector using the field name and allocator.
          vector = ListVector.empty(colName, ALLOCATOR);
          // Initialize its children from the defined field.
          vector.initializeChildrenFromFields(children);
          break;
        case DOUBLE_ARRAY:
          // Define the inner field for a double element.
          children = Collections.singletonList(
              new Field("element", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                  null));
          // Define the field for the list column.
          field = new Field(colName, FieldType.nullable(new ArrowType.List()), children);
          // Create a ListVector using the field name and allocator.
          vector = ListVector.empty(colName, ALLOCATOR);
          // Initialize its children from the defined field.
          vector.initializeChildrenFromFields(children);
          break;
        case TIMESTAMP_ARRAY:
        case STRING_ARRAY:
        case BYTES_ARRAY:
          // Define the inner field for a string element.
          children = Collections.singletonList(new Field("element", FieldType.nullable(new ArrowType.Utf8()), null));
          // Define the field for the list column.
          field = new Field(colName, FieldType.nullable(new ArrowType.List()), children);
          // Create a ListVector using the field name and allocator.
          vector = ListVector.empty(colName, ALLOCATOR);
          // Initialize its children from the defined field.
          vector.initializeChildrenFromFields(children);
          break;
        default:
          throw new UnsupportedOperationException("Unsupported column type: " + colType);
      }
      fields.add(field);
      vector.allocateNew();
      vectors.add(vector);
    }

    // Determine the actual end row.
    int availableRows = resultTable.getRows().size();
    int endRow = Math.min(startRow + length, availableRows);

    // Write the rows from startRow to endRow into the corresponding Arrow vectors.
    for (int i = startRow; i < endRow; i++) {
      Object[] row = resultTable.getRows().get(i);
      int rowIndex = i - startRow;
      for (int col = 0; col < numColumns; col++) {
        FieldVector vector = vectors.get(col);
        Object value = row[col];
        if (value == null) {
          vector.setNull(rowIndex);
        } else {
          DataSchema.ColumnDataType colType = schema.getColumnDataTypes()[col];
          switch (colType) {
            case BOOLEAN:
              ((org.apache.arrow.vector.BitVector) vector).setSafe(rowIndex, ((Boolean) value) ? 1 : 0);
              break;
            case INT:
              ((IntVector) vector).setSafe(rowIndex, ((Number) value).intValue());
              break;
            case LONG:
              ((BigIntVector) vector).setSafe(rowIndex, ((Number) value).longValue());
              break;
            case FLOAT:
              ((Float4Vector) vector).setSafe(rowIndex, ((Number) value).floatValue());
              break;
            case DOUBLE:
              ((Float8Vector) vector).setSafe(rowIndex, ((Number) value).doubleValue());
              break;
            case TIMESTAMP:
            case STRING:
            case BYTES:
            case BIG_DECIMAL:
            case JSON:
            case OBJECT:
              byte[] bytes = ((String) value).getBytes(StandardCharsets.UTF_8);
              ((VarCharVector) vector).setSafe(rowIndex, bytes);
              break;
            case MAP:
              byte[] mapValueBytes = MapUtils.serializeMap((Map) value);
              ((VarBinaryVector) vector).setSafe(rowIndex, mapValueBytes);
              break;
            case UNKNOWN:
              // Handle null value
              vector.setNull(rowIndex);
              break;
            case BOOLEAN_ARRAY:
              ListVector booleanArrayVector = (ListVector) vector;
              boolean[] booleanArray = (boolean[]) value;
              // Start a new list entry for the current row.
              booleanArrayVector.startNewValue(rowIndex);
              // Retrieve the underlying child vector (BitVector).
              BitVector bitVector = (BitVector) booleanArrayVector.getDataVector();
              // Determine the current position in the child vector.
              int bitVectorIndex = bitVector.getValueCount();
              // Write each boolean into the child vector.
              for (boolean v : booleanArray) {
                bitVector.setSafe(bitVectorIndex, v ? 1 : 0);
                bitVectorIndex++;
              }
              // Update the child vector's value count.
              bitVector.setValueCount(bitVectorIndex);
              // Finalize the list entry by indicating the number of elements added.
              booleanArrayVector.endValue(rowIndex, booleanArray.length);
              break;
            case INT_ARRAY:
              ListVector intArrayVector = (ListVector) vector;
              int[] intArr = (int[]) value;
              // Start a new list entry for the current row.
              intArrayVector.startNewValue(rowIndex);
              // Retrieve the underlying child vector (IntVector).
              IntVector intVector = (IntVector) intArrayVector.getDataVector();
              // Determine the current position in the child vector.
              int intVectorIndex = intVector.getValueCount();
              // Write each integer into the child vector.
              for (int v : intArr) {
                intVector.setSafe(intVectorIndex, v);
                intVectorIndex++;
              }
              // Update the child vector's value count.
              intVector.setValueCount(intVectorIndex);
              // Finalize the list entry by indicating the number of elements added.
              intArrayVector.endValue(rowIndex, intArr.length);
              break;
            case LONG_ARRAY:
              ListVector longArrayVector = (ListVector) vector;
              long[] longArray = (long[]) value;
              // Start a new list entry for the current row.
              longArrayVector.startNewValue(rowIndex);
              // Retrieve the underlying child vector (BigIntVector).
              BigIntVector bigIntVector = (BigIntVector) longArrayVector.getDataVector();
              // Determine the current position in the child vector.
              int bigIntVectorIndex = bigIntVector.getValueCount();
              // Write each long into the child vector.
              for (long v : longArray) {
                bigIntVector.setSafe(bigIntVectorIndex, v);
                bigIntVectorIndex++;
              }
              // Update the child vector's value count.
              bigIntVector.setValueCount(bigIntVectorIndex);
              // Finalize the list entry by indicating the number of elements added.
              longArrayVector.endValue(rowIndex, longArray.length);
              break;
            case FLOAT_ARRAY:
              ListVector floatArrayVector = (ListVector) vector;
              float[] floatArray = (float[]) value;
              // Start a new list entry for the current row.
              floatArrayVector.startNewValue(rowIndex);
              // Retrieve the underlying child vector (Float4Vector).
              Float4Vector floatVector = (Float4Vector) floatArrayVector.getDataVector();
              // Determine the current position in the child vector.
              int floatVectorIndex = floatVector.getValueCount();
              // Write each float into the child vector.
              for (float v : floatArray) {
                floatVector.setSafe(floatVectorIndex, v);
                floatVectorIndex++;
              }
              // Update the child vector's value count.
              floatVector.setValueCount(floatVectorIndex);
              // Finalize the list entry by indicating the number of elements added.
              floatArrayVector.endValue(rowIndex, floatArray.length);
              break;
            case DOUBLE_ARRAY:
              ListVector doubleArrayVector = (ListVector) vector;
              double[] doubleArray = (double[]) value;
              // Start a new list entry for the current row.
              doubleArrayVector.startNewValue(rowIndex);
              // Retrieve the underlying child vector (Float8Vector).
              Float8Vector doubleVector = (Float8Vector) doubleArrayVector.getDataVector();
              // Determine the current position in the child vector.
              int doubleVectorIndex = doubleVector.getValueCount();
              // Write each double into the child vector.
              for (double v : doubleArray) {
                doubleVector.setSafe(doubleVectorIndex, v);
                doubleVectorIndex++;
              }
              // Update the child vector's value count.
              doubleVector.setValueCount(doubleVectorIndex);
              // Finalize the list entry by indicating the number of elements added.
              doubleArrayVector.endValue(rowIndex, doubleArray.length);
              break;
            case TIMESTAMP_ARRAY:
            case STRING_ARRAY:
            case BYTES_ARRAY:
              ListVector listVector = (ListVector) vector;
              String[] stringArray = (String[]) value;
              // Start a new list entry for the current row.
              listVector.startNewValue(rowIndex);
              // Retrieve the underlying child vector (VarCharVector).
              VarCharVector stringVector = (VarCharVector) listVector.getDataVector();
              // Determine the current position in the child vector.
              int stringVectorIndex = stringVector.getValueCount();
              // Write each string into the child vector.
              for (String v : stringArray) {
                byte[] stringBytes = v.getBytes(StandardCharsets.UTF_8);
                stringVector.setSafe(stringVectorIndex, stringBytes);
                stringVectorIndex++;
              }
              // Update the child vector's value count.
              stringVector.setValueCount(stringVectorIndex);
              // Finalize the list entry by indicating the number of elements added.
              listVector.endValue(rowIndex, stringArray.length);
              break;
            default:
              throw new UnsupportedOperationException("Unsupported column type: " + colType);
          }
        }
      }
    }

    // Set the value count on each vector.
    int numRows = endRow - startRow;
    for (FieldVector vector : vectors) {
      vector.setValueCount(numRows);
    }

    Schema arrowSchema = new Schema(fields);
    return new VectorSchemaRoot(arrowSchema, vectors, numRows);
  }

  @Override
  public ResultTable decodeResultTable(byte[] bytes, int rowSize, DataSchema schema)
      throws IOException {
    try (ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        ArrowStreamReader reader = new ArrowStreamReader(in, ALLOCATOR)) {
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      // Read the first (and only) batch.
      reader.loadNextBatch();
      int numRows = root.getRowCount();
      int numColumns = schema.getColumnNames().length;
      List<Object[]> rows = new ArrayList<>();

      // For each row, retrieve each column value from the corresponding vector.
      for (int i = 0; i < numRows; i++) {
        Object[] row = new Object[numColumns];
        for (int col = 0; col < numColumns; col++) {
          FieldVector vector = root.getVector(col);
          switch (schema.getColumnDataType(col)) {
            case BOOLEAN:
              row[col] = ((BitVector) vector).get(i) == 1;
              break;
            case TIMESTAMP:
            case STRING:
            case BYTES:
            case BIG_DECIMAL:
            case JSON:
            case OBJECT:
              row[col] = new String(((VarCharVector) vector).get(i), StandardCharsets.UTF_8);
              break;
            case MAP:
              row[col] = MapUtils.deserializeMap(((VarBinaryVector) vector).get(i));
              break;
            case UNKNOWN:
              row[col] = null;
              break;
            case BOOLEAN_ARRAY:
              ListVector booleanArrayListVector = (ListVector) vector;
              JsonStringArrayList booleanArrayList = (JsonStringArrayList) booleanArrayListVector.getObject(i);
              boolean[] booleanArray = new boolean[booleanArrayList.size()];
              for (int j = 0; j < booleanArrayList.size(); j++) {
                booleanArray[j] = (boolean) booleanArrayList.get(j);
              }
              row[col] = booleanArray;
              break;
            case INT_ARRAY:
              ListVector intArrayListVector = (ListVector) vector;
              List<?> intArrayValues = intArrayListVector.getObject(i);
              int[] intArray = new int[intArrayValues.size()];
              for (int j = 0; j < intArrayValues.size(); j++) {
                intArray[j] = (int) intArrayValues.get(j);
              }
              row[col] = intArray;
              break;
            case LONG_ARRAY:
              ListVector longArrayListVector = (ListVector) vector;
              List<?> longArrayValues = longArrayListVector.getObject(i);
              long[] longArray = new long[longArrayValues.size()];
              for (int j = 0; j < longArrayValues.size(); j++) {
                longArray[j] = (long) longArrayValues.get(j);
              }
              row[col] = longArray;
              break;
            case FLOAT_ARRAY:
              ListVector floatArrayListVector = (ListVector) vector;
              List<?> floatArrayValues = floatArrayListVector.getObject(i);
              float[] floatArray = new float[floatArrayValues.size()];
              for (int j = 0; j < floatArrayValues.size(); j++) {
                floatArray[j] = (float) floatArrayValues.get(j);
              }
              row[col] = floatArray;
              break;
            case DOUBLE_ARRAY:
              ListVector doubleArrayListVector = (ListVector) vector;
              List<?> doubleArrayValues = doubleArrayListVector.getObject(i);
              double[] doubleArray = new double[doubleArrayValues.size()];
              for (int j = 0; j < doubleArrayValues.size(); j++) {
                doubleArray[j] = (double) doubleArrayValues.get(j);
              }
              row[col] = doubleArray;
              break;
            case TIMESTAMP_ARRAY:
            case STRING_ARRAY:
            case BYTES_ARRAY:
              ListVector listVector = (ListVector) vector;
              List<?> arrayValues = listVector.getObject(i);
              String[] array = new String[arrayValues.size()];
              for (int j = 0; j < arrayValues.size(); j++) {
                array[j] = arrayValues.get(j).toString();
              }
              row[col] = array;
              break;
            default:
              row[col] = vector.getObject(i);
              break;
          }
        }
        rows.add(row);
      }
      // Construct a Pinot ResultTable from the DataSchema and rows.
      return new ResultTable(schema, rows);
    }
  }
}
