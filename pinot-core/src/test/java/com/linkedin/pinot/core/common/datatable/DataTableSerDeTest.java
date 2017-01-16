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

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.common.utils.DataSchema;
import com.linkedin.pinot.common.utils.DataTable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for {@link DataTable} serialization/de-serialization.
 */
public class DataTableSerDeTest {
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);
  private static final String ERROR_MESSAGE = "Random seed: " + RANDOM_SEED;

  private static final int NUM_ROWS = 100;

  @Test
  public void testException()
      throws IOException {
    Exception exception = new UnsupportedOperationException("Caught exception.");
    ProcessingException processingException =
        QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, exception);
    String expected = processingException.getMessage();

    DataTable dataTable = new DataTableImplV2();
    dataTable.addException(processingException);
    DataTable newDataTable = DataTableFactory.getDataTable(dataTable.toBytes());
    Assert.assertNull(newDataTable.getDataSchema());
    Assert.assertEquals(newDataTable.getNumberOfRows(), 0);

    String actual = newDataTable.getMetadata()
        .get(DataTable.EXCEPTION_METADATA_KEY + QueryException.QUERY_EXECUTION_ERROR.getErrorCode());
    Assert.assertEquals(actual, expected);
  }

  @Test
  public void testAllDataTypes()
      throws IOException {
    DataType[] columnTypes = DataType.values();
    int numColumns = columnTypes.length;
    String[] columnNames = new String[numColumns];
    for (int i = 0; i < numColumns; i++) {
      columnNames[i] = columnTypes[i].name();
    }
    DataSchema dataSchema = new DataSchema(columnNames, columnTypes);

    DataTableBuilder dataTableBuilder = new DataTableBuilder(dataSchema);

    boolean[] booleans = new boolean[NUM_ROWS];
    byte[] bytes = new byte[NUM_ROWS];
    char[] chars = new char[NUM_ROWS];
    short[] shorts = new short[NUM_ROWS];
    int[] ints = new int[NUM_ROWS];
    long[] longs = new long[NUM_ROWS];
    float[] floats = new float[NUM_ROWS];
    double[] doubles = new double[NUM_ROWS];
    String[] strings = new String[NUM_ROWS];
    Object[] objects = new Object[NUM_ROWS];
    byte[][] byteArrays = new byte[NUM_ROWS][];
    char[][] charArrays = new char[NUM_ROWS][];
    short[][] shortArrays = new short[NUM_ROWS][];
    int[][] intArrays = new int[NUM_ROWS][];
    long[][] longArrays = new long[NUM_ROWS][];
    float[][] floatArrays = new float[NUM_ROWS][];
    double[][] doubleArrays = new double[NUM_ROWS][];
    String[][] stringArrays = new String[NUM_ROWS][];

    for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
      dataTableBuilder.startRow();
      for (int colId = 0; colId < numColumns; colId++) {
        switch (columnTypes[colId]) {
          case BOOLEAN:
            booleans[rowId] = RANDOM.nextBoolean();
            dataTableBuilder.setColumn(colId, booleans[rowId]);
            break;
          case BYTE:
            bytes[rowId] = (byte) RANDOM.nextInt();
            dataTableBuilder.setColumn(colId, bytes[rowId]);
            break;
          case CHAR:
            chars[rowId] = (char) RANDOM.nextInt();
            dataTableBuilder.setColumn(colId, chars[rowId]);
            break;
          case SHORT:
            shorts[rowId] = (short) RANDOM.nextInt();
            dataTableBuilder.setColumn(colId, shorts[rowId]);
            break;
          case INT:
            ints[rowId] = RANDOM.nextInt();
            dataTableBuilder.setColumn(colId, ints[rowId]);
            break;
          case LONG:
            longs[rowId] = RANDOM.nextLong();
            dataTableBuilder.setColumn(colId, longs[rowId]);
            break;
          case FLOAT:
            floats[rowId] = RANDOM.nextFloat();
            dataTableBuilder.setColumn(colId, floats[rowId]);
            break;
          case DOUBLE:
            doubles[rowId] = RANDOM.nextDouble();
            dataTableBuilder.setColumn(colId, doubles[rowId]);
            break;
          case STRING:
            strings[rowId] = RandomStringUtils.random(RANDOM.nextInt(20));
            dataTableBuilder.setColumn(colId, strings[rowId]);
            break;
          // Just test Double here, all object types will be covered in ObjectCustomSerDeTest.
          case OBJECT:
            objects[rowId] = RANDOM.nextDouble();
            dataTableBuilder.setColumn(colId, objects[rowId]);
            break;
          case BYTE_ARRAY:
            int length = RANDOM.nextInt(20);
            byte[] byteArray = new byte[length];
            for (int i = 0; i < length; i++) {
              byteArray[i] = (byte) RANDOM.nextInt();
            }
            byteArrays[rowId] = byteArray;
            dataTableBuilder.setColumn(colId, byteArray);
            break;
          case CHAR_ARRAY:
            length = RANDOM.nextInt(20);
            char[] charArray = new char[length];
            for (int i = 0; i < length; i++) {
              charArray[i] = (char) RANDOM.nextInt();
            }
            charArrays[rowId] = charArray;
            dataTableBuilder.setColumn(colId, charArray);
            break;
          case SHORT_ARRAY:
            length = RANDOM.nextInt(20);
            short[] shortArray = new short[length];
            for (int i = 0; i < length; i++) {
              shortArray[i] = (short) RANDOM.nextInt();
            }
            shortArrays[rowId] = shortArray;
            dataTableBuilder.setColumn(colId, shortArray);
            break;
          case INT_ARRAY:
            length = RANDOM.nextInt(20);
            int[] intArray = new int[length];
            for (int i = 0; i < length; i++) {
              intArray[i] = RANDOM.nextInt();
            }
            intArrays[rowId] = intArray;
            dataTableBuilder.setColumn(colId, intArray);
            break;
          case LONG_ARRAY:
            length = RANDOM.nextInt(20);
            long[] longArray = new long[length];
            for (int i = 0; i < length; i++) {
              longArray[i] = RANDOM.nextLong();
            }
            longArrays[rowId] = longArray;
            dataTableBuilder.setColumn(colId, longArray);
            break;
          case FLOAT_ARRAY:
            length = RANDOM.nextInt(20);
            float[] floatArray = new float[length];
            for (int i = 0; i < length; i++) {
              floatArray[i] = RANDOM.nextFloat();
            }
            floatArrays[rowId] = floatArray;
            dataTableBuilder.setColumn(colId, floatArray);
            break;
          case DOUBLE_ARRAY:
            length = RANDOM.nextInt(20);
            double[] doubleArray = new double[length];
            for (int i = 0; i < length; i++) {
              doubleArray[i] = RANDOM.nextDouble();
            }
            doubleArrays[rowId] = doubleArray;
            dataTableBuilder.setColumn(colId, doubleArray);
            break;
          case STRING_ARRAY:
            length = RANDOM.nextInt(20);
            String[] stringArray = new String[length];
            for (int i = 0; i < length; i++) {
              stringArray[i] = RandomStringUtils.random(RANDOM.nextInt(20));
            }
            stringArrays[rowId] = stringArray;
            dataTableBuilder.setColumn(colId, stringArray);
            break;
        }
      }
      dataTableBuilder.finishRow();
    }

    DataTable dataTable = dataTableBuilder.build();
    DataTable newDataTable = DataTableFactory.getDataTable(dataTable.toBytes());
    Assert.assertEquals(newDataTable.getDataSchema(), dataSchema, ERROR_MESSAGE);
    Assert.assertEquals(newDataTable.getNumberOfRows(), NUM_ROWS, ERROR_MESSAGE);

    for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
      for (int colId = 0; colId < numColumns; colId++) {
        switch (columnTypes[colId]) {
          case BOOLEAN:
            Assert.assertEquals(newDataTable.getBoolean(rowId, colId), booleans[rowId], ERROR_MESSAGE);
            break;
          case BYTE:
            Assert.assertEquals(newDataTable.getByte(rowId, colId), bytes[rowId], ERROR_MESSAGE);
            break;
          case CHAR:
            Assert.assertEquals(newDataTable.getChar(rowId, colId), chars[rowId], ERROR_MESSAGE);
            break;
          case SHORT:
            Assert.assertEquals(newDataTable.getShort(rowId, colId), shorts[rowId], ERROR_MESSAGE);
            break;
          case INT:
            Assert.assertEquals(newDataTable.getInt(rowId, colId), ints[rowId], ERROR_MESSAGE);
            break;
          case LONG:
            Assert.assertEquals(newDataTable.getLong(rowId, colId), longs[rowId], ERROR_MESSAGE);
            break;
          case FLOAT:
            Assert.assertEquals(newDataTable.getFloat(rowId, colId), floats[rowId], ERROR_MESSAGE);
            break;
          case DOUBLE:
            Assert.assertEquals(newDataTable.getDouble(rowId, colId), doubles[rowId], ERROR_MESSAGE);
            break;
          case STRING:
            Assert.assertEquals(newDataTable.getString(rowId, colId), strings[rowId], ERROR_MESSAGE);
            break;
          case OBJECT:
            Assert.assertEquals(newDataTable.getObject(rowId, colId), objects[rowId], ERROR_MESSAGE);
            break;
          case BYTE_ARRAY:
            Assert.assertTrue(Arrays.equals(newDataTable.getByteArray(rowId, colId), byteArrays[rowId]), ERROR_MESSAGE);
            break;
          case CHAR_ARRAY:
            Assert.assertTrue(Arrays.equals(newDataTable.getCharArray(rowId, colId), charArrays[rowId]), ERROR_MESSAGE);
            break;
          case SHORT_ARRAY:
            Assert.assertTrue(Arrays.equals(newDataTable.getShortArray(rowId, colId), shortArrays[rowId]),
                ERROR_MESSAGE);
            break;
          case INT_ARRAY:
            Assert.assertTrue(Arrays.equals(newDataTable.getIntArray(rowId, colId), intArrays[rowId]), ERROR_MESSAGE);
            break;
          case LONG_ARRAY:
            Assert.assertTrue(Arrays.equals(newDataTable.getLongArray(rowId, colId), longArrays[rowId]), ERROR_MESSAGE);
            break;
          case FLOAT_ARRAY:
            Assert.assertTrue(Arrays.equals(newDataTable.getFloatArray(rowId, colId), floatArrays[rowId]),
                ERROR_MESSAGE);
            break;
          case DOUBLE_ARRAY:
            Assert.assertTrue(Arrays.equals(newDataTable.getDoubleArray(rowId, colId), doubleArrays[rowId]),
                ERROR_MESSAGE);
            break;
          case STRING_ARRAY:
            Assert.assertTrue(Arrays.equals(newDataTable.getStringArray(rowId, colId), stringArrays[rowId]),
                ERROR_MESSAGE);
            break;
        }
      }
    }
  }
}
