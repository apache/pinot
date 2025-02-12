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
package org.apache.pinot.core.common.datatable;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Unit test for {@link DataTable} serialization/de-serialization.
 */
public class DataTableSerDeTest {
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);
  private static final String ERROR_MESSAGE = "Random seed: " + RANDOM_SEED;

  private static final int NUM_ROWS = 100;

  private static final int[] INTS = new int[NUM_ROWS];
  private static final long[] LONGS = new long[NUM_ROWS];
  private static final float[] FLOATS = new float[NUM_ROWS];
  private static final double[] DOUBLES = new double[NUM_ROWS];
  private static final BigDecimal[] BIG_DECIMALS = new BigDecimal[NUM_ROWS];
  private static final int[] BOOLEANS = new int[NUM_ROWS];
  private static final long[] TIMESTAMPS = new long[NUM_ROWS];
  private static final String[] STRINGS = new String[NUM_ROWS];
  private static final String[] JSONS = new String[NUM_ROWS];
  private static final byte[][] BYTES = new byte[NUM_ROWS][];
  private static final Object[] OBJECTS = new Object[NUM_ROWS];
  private static final int[][] INT_ARRAYS = new int[NUM_ROWS][];
  private static final long[][] LONG_ARRAYS = new long[NUM_ROWS][];
  private static final float[][] FLOAT_ARRAYS = new float[NUM_ROWS][];
  private static final double[][] DOUBLE_ARRAYS = new double[NUM_ROWS][];
  private static final int[][] BOOLEAN_ARRAYS = new int[NUM_ROWS][];
  private static final long[][] TIMESTAMP_ARRAYS = new long[NUM_ROWS][];
  private static final String[][] STRING_ARRAYS = new String[NUM_ROWS][];
  private static final Map<String, Object>[] MAPS = new Map[NUM_ROWS];

  @Test(dataProvider = "versionProvider")
  public void testException(int dataTableVersion)
      throws IOException {
    DataTableBuilderFactory.setDataTableVersion(dataTableVersion);
    String expected = "Caught exception.";

    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.addException(QueryErrorCode.QUERY_EXECUTION, expected);
    DataTable newDataTable = DataTableFactory.getDataTable(dataTable.toBytes());
    Assert.assertNull(newDataTable.getDataSchema());
    Assert.assertEquals(newDataTable.getNumberOfRows(), 0);

    String actual = newDataTable.getExceptions().get(QueryErrorCode.QUERY_EXECUTION.getId());
    Assert.assertEquals(actual, expected);
  }

  @Test(dataProvider = "versionProvider")
  public void testEmptyValues(int dataTableVersion)
      throws IOException {
    DataTableBuilderFactory.setDataTableVersion(dataTableVersion);
    String emptyString = StringUtils.EMPTY;
    String[] emptyStringArray = {StringUtils.EMPTY};
    ByteArray emptyBytes = new ByteArray(new byte[0]);
    for (int numRows = 0; numRows < NUM_ROWS; numRows++) {
      testEmptyValues(new DataSchema(new String[]{"STR_SV", "STR_MV"}, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING_ARRAY
      }), numRows, new Object[]{emptyString, emptyStringArray});

      testEmptyValues(
          new DataSchema(new String[]{"STR_SV"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING}),
          numRows, new Object[]{emptyString});

      testEmptyValues(new DataSchema(new String[]{"STR_MV"},
              new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING_ARRAY}), numRows,
          new Object[]{emptyStringArray});

      testEmptyValues(
          new DataSchema(new String[]{"BYTES"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.BYTES}),
          numRows, new Object[]{emptyBytes});

      testEmptyValues(new DataSchema(new String[]{"BOOL_ARR"},
              new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.BOOLEAN_ARRAY}), numRows,
          new Object[]{new int[]{}});

      testEmptyValues(new DataSchema(new String[]{"BOOL_ARR"},
              new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.BOOLEAN_ARRAY}), numRows,
          new Object[]{new int[]{0}});

      testEmptyValues(
          new DataSchema(new String[]{"INT_ARR"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT_ARRAY}),
          numRows, new Object[]{new int[]{}});

      testEmptyValues(
          new DataSchema(new String[]{"INT_ARR"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT_ARRAY}),
          numRows, new Object[]{new int[]{0}});

      testEmptyValues(new DataSchema(new String[]{"LONG_ARR"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG_ARRAY}), numRows, new Object[]{new long[]{}});

      testEmptyValues(new DataSchema(new String[]{"LONG_ARR"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG_ARRAY}), numRows, new Object[]{new long[]{0}});

      testEmptyValues(new DataSchema(new String[]{"FLOAT_ARR"},
              new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.FLOAT_ARRAY}), numRows,
          new Object[]{new float[]{}});

      testEmptyValues(new DataSchema(new String[]{"FLOAT_ARR"},
              new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.FLOAT_ARRAY}), numRows,
          new Object[]{new float[]{0}});

      testEmptyValues(new DataSchema(new String[]{"DOUBLE_ARR"},
              new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE_ARRAY}), numRows,
          new Object[]{new double[]{}});

      testEmptyValues(new DataSchema(new String[]{"DOUBLE_ARR"},
              new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE_ARRAY}), numRows,
          new Object[]{new double[]{0}});
    }
  }

  private void testEmptyValues(DataSchema dataSchema, int numRows, Object[] emptyValues)
      throws IOException {

    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    for (int rowId = 0; rowId < numRows; rowId++) {
      dataTableBuilder.startRow();
      for (int columnId = 0; columnId < dataSchema.size(); columnId++) {
        Object emptyValue = emptyValues[columnId];
        if (emptyValue instanceof int[]) {
          dataTableBuilder.setColumn(columnId, (int[]) emptyValue);
        } else if (emptyValue instanceof long[]) {
          dataTableBuilder.setColumn(columnId, (long[]) emptyValue);
        } else if (emptyValue instanceof float[]) {
          dataTableBuilder.setColumn(columnId, (float[]) emptyValue);
        } else if (emptyValue instanceof double[]) {
          dataTableBuilder.setColumn(columnId, (double[]) emptyValue);
        } else if (emptyValue instanceof String[]) {
          dataTableBuilder.setColumn(columnId, (String[]) emptyValue);
        } else if (emptyValue instanceof String) {
          dataTableBuilder.setColumn(columnId, (String) emptyValue);
        } else if (emptyValue instanceof ByteArray) {
          dataTableBuilder.setColumn(columnId, (ByteArray) emptyValue);
        } else {
          dataTableBuilder.setColumn(columnId, emptyValue);
        }
      }
      dataTableBuilder.finishRow();
    }

    DataTable dataTable = dataTableBuilder.build();
    DataTable newDataTable = DataTableFactory.getDataTable(dataTable.toBytes());
    Assert.assertEquals(newDataTable.getDataSchema(), dataSchema);
    Assert.assertEquals(newDataTable.getNumberOfRows(), numRows);

    for (int rowId = 0; rowId < numRows; rowId++) {
      for (int columnId = 0; columnId < dataSchema.size(); columnId++) {
        Object entry;
        switch (dataSchema.getColumnDataType(columnId)) {
          case BOOLEAN_ARRAY:
          case INT_ARRAY:
            entry = newDataTable.getIntArray(rowId, columnId);
            break;
          case LONG_ARRAY:
            entry = newDataTable.getLongArray(rowId, columnId);
            break;
          case FLOAT_ARRAY:
            entry = newDataTable.getFloatArray(rowId, columnId);
            break;
          case DOUBLE_ARRAY:
            entry = newDataTable.getDoubleArray(rowId, columnId);
            break;
          case STRING_ARRAY:
            entry = newDataTable.getStringArray(rowId, columnId);
            break;
          case STRING:
            entry = newDataTable.getString(rowId, columnId);
            break;
          case BYTES:
            entry = newDataTable.getBytes(rowId, columnId);
            break;
          default:
            entry = newDataTable.getCustomObject(rowId, columnId);
            break;
        }
        Assert.assertEquals(entry, emptyValues[columnId]);
      }
    }
  }

  @Test(dataProvider = "versionProvider")
  public void testAllDataTypesInOneSchema(int dataTableVersion)
      throws IOException {
    DataTableBuilderFactory.setDataTableVersion(dataTableVersion);
    DataSchema.ColumnDataType[] columnDataTypes = DataSchema.ColumnDataType.values();
    int numColumns = columnDataTypes.length;
    String[] columnNames = new String[numColumns];
    for (int i = 0; i < numColumns; i++) {
      columnNames[i] = columnDataTypes[i].name();
    }

    DataSchema dataSchema = new DataSchema(columnNames, columnDataTypes);
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    fillDataTableWithRandomData(dataTableBuilder, columnDataTypes, numColumns);

    DataTable dataTable = dataTableBuilder.build();
    DataTable newDataTable = DataTableFactory.getDataTable(dataTable.toBytes());
    Assert.assertEquals(newDataTable.getDataSchema(), dataSchema, ERROR_MESSAGE);
    Assert.assertEquals(newDataTable.getNumberOfRows(), NUM_ROWS, ERROR_MESSAGE);
    verifyDataIsSame(newDataTable, columnDataTypes, numColumns);
  }

  @Test(dataProvider = "versionProvider")
  public void testAllDataTypes(int dataTableVersion)
      throws IOException {
    DataTableBuilderFactory.setDataTableVersion(dataTableVersion);
    DataSchema.ColumnDataType[] columnDataTypes = DataSchema.ColumnDataType.values();
    int numColumns = columnDataTypes.length;
    for (int i = 0; i < numColumns; i++) {
      String[] columnName = new String[]{columnDataTypes[i].name()};
      DataSchema.ColumnDataType[] columnDataType = new DataSchema.ColumnDataType[]{columnDataTypes[i]};
      DataSchema dataSchema = new DataSchema(columnName, columnDataType);
      for (int numRows = 0; numRows < NUM_ROWS; numRows++) {
        DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
        fillDataTableWithRandomData(dataTableBuilder, columnDataType, 1, numRows);
        DataTable dataTable = dataTableBuilder.build();
        DataTable newDataTable = DataTableFactory.getDataTable(dataTable.toBytes());
        Assert.assertEquals(newDataTable.getDataSchema(), dataSchema, ERROR_MESSAGE);
        Assert.assertEquals(newDataTable.getNumberOfRows(), numRows, ERROR_MESSAGE);
        verifyDataIsSame(newDataTable, columnDataType, 1, numRows);
      }
    }
  }

  @Test(dataProvider = "versionProvider")
  public void testExecutionThreadCpuTimeNs(int dataTableVersion)
      throws IOException {
    DataTableBuilderFactory.setDataTableVersion(dataTableVersion);
    DataSchema.ColumnDataType[] columnDataTypes = DataSchema.ColumnDataType.values();
    int numColumns = columnDataTypes.length;
    String[] columnNames = new String[numColumns];
    for (int i = 0; i < numColumns; i++) {
      columnNames[i] = columnDataTypes[i].name();
    }

    DataSchema dataSchema = new DataSchema(columnNames, columnDataTypes);
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    fillDataTableWithRandomData(dataTableBuilder, columnDataTypes, numColumns);

    DataTable dataTable = dataTableBuilder.build();

    // Disable ThreadCpuTimeMeasurement, serialize/de-serialize data table.
    ThreadResourceUsageProvider.setThreadCpuTimeMeasurementEnabled(false);
    DataTable newDataTable = DataTableFactory.getDataTable(dataTable.toBytes());
    // When ThreadCpuTimeMeasurement is disabled, no value for
    // threadCpuTimeNs/systemActivitiesCpuTimeNs/responseSerializationCpuTimeNs.
    Assert.assertNull(newDataTable.getMetadata().get(MetadataKey.THREAD_CPU_TIME_NS.getName()));
    Assert.assertNull(newDataTable.getMetadata().get(MetadataKey.SYSTEM_ACTIVITIES_CPU_TIME_NS.getName()));
    Assert.assertNull(newDataTable.getMetadata().get(MetadataKey.RESPONSE_SER_CPU_TIME_NS.getName()));

    // Enable ThreadCpuTimeMeasurement, serialize/de-serialize data table.
    ThreadResourceUsageProvider.setThreadCpuTimeMeasurementEnabled(true);
    newDataTable = DataTableFactory.getDataTable(dataTable.toBytes());
    // When ThreadCpuTimeMeasurement is enabled, value of responseSerializationCpuTimeNs is not 0.
    Assert.assertNull(newDataTable.getMetadata().get(MetadataKey.THREAD_CPU_TIME_NS.getName()));
    Assert.assertNull(newDataTable.getMetadata().get(MetadataKey.SYSTEM_ACTIVITIES_CPU_TIME_NS.getName()));
    Assert.assertTrue(
        Integer.parseInt(newDataTable.getMetadata().get(MetadataKey.RESPONSE_SER_CPU_TIME_NS.getName())) > 0);
  }

  private void fillDataTableWithRandomData(DataTableBuilder dataTableBuilder,
      DataSchema.ColumnDataType[] columnDataTypes, int numColumns)
      throws IOException {
    fillDataTableWithRandomData(dataTableBuilder, columnDataTypes, numColumns, NUM_ROWS);
  }

  private void fillDataTableWithRandomData(DataTableBuilder dataTableBuilder,
      DataSchema.ColumnDataType[] columnDataTypes, int numColumns, int numRows)
      throws IOException {
    RoaringBitmap[] nullBitmaps = new RoaringBitmap[numColumns];
    for (int colId = 0; colId < numColumns; colId++) {
      nullBitmaps[colId] = new RoaringBitmap();
    }
    for (int rowId = 0; rowId < numRows; rowId++) {
      dataTableBuilder.startRow();
      for (int colId = 0; colId < numColumns; colId++) {
        // Note: isNull is handled for SV columns only for now.
        boolean isNull = RANDOM.nextFloat() < 0.1;
        if (isNull) {
          nullBitmaps[colId].add(rowId);
        }
        switch (columnDataTypes[colId]) {
          case INT:
            INTS[rowId] = isNull ? 0 : RANDOM.nextInt();
            dataTableBuilder.setColumn(colId, INTS[rowId]);
            break;
          case LONG:
            LONGS[rowId] = isNull ? 0 : RANDOM.nextLong();
            dataTableBuilder.setColumn(colId, LONGS[rowId]);
            break;
          case FLOAT:
            FLOATS[rowId] = isNull ? 0 : RANDOM.nextFloat();
            dataTableBuilder.setColumn(colId, FLOATS[rowId]);
            break;
          case DOUBLE:
            DOUBLES[rowId] = isNull ? 0.0 : RANDOM.nextDouble();
            dataTableBuilder.setColumn(colId, DOUBLES[rowId]);
            break;
          case BIG_DECIMAL:
            BIG_DECIMALS[rowId] = isNull ? BigDecimal.ZERO : BigDecimal.valueOf(RANDOM.nextDouble());
            dataTableBuilder.setColumn(colId, BIG_DECIMALS[rowId]);
            break;
          case TIMESTAMP:
            TIMESTAMPS[rowId] = isNull ? 0 : RANDOM.nextLong();
            dataTableBuilder.setColumn(colId, TIMESTAMPS[rowId]);
            break;
          case BOOLEAN:
            BOOLEANS[rowId] = isNull ? 0 : RANDOM.nextInt(2);
            dataTableBuilder.setColumn(colId, BOOLEANS[rowId]);
            break;
          case STRING:
            STRINGS[rowId] = isNull ? "" : RandomStringUtils.random(RANDOM.nextInt(20));
            dataTableBuilder.setColumn(colId, STRINGS[rowId]);
            break;
          case JSON:
            JSONS[rowId] = isNull ? "" : "{\"key\": \"" + RandomStringUtils.random(RANDOM.nextInt(20)) + "\"}";
            dataTableBuilder.setColumn(colId, JSONS[rowId]);
            break;
          case BYTES:
            BYTES[rowId] = isNull ? new byte[0] : RandomStringUtils.random(RANDOM.nextInt(20)).getBytes();
            dataTableBuilder.setColumn(colId, new ByteArray(BYTES[rowId]));
            break;
          // Just test Double here, all object types will be covered in ObjectCustomSerDeTest.
          case OBJECT:
            OBJECTS[rowId] = isNull ? null : RANDOM.nextDouble();
            dataTableBuilder.setColumn(colId, OBJECTS[rowId]);
            break;
          case INT_ARRAY:
            int length = RANDOM.nextInt(20);
            int[] intArray = new int[length];
            for (int i = 0; i < length; i++) {
              intArray[i] = RANDOM.nextInt();
            }
            INT_ARRAYS[rowId] = intArray;
            dataTableBuilder.setColumn(colId, intArray);
            break;
          case LONG_ARRAY:
            length = RANDOM.nextInt(20);
            long[] longArray = new long[length];
            for (int i = 0; i < length; i++) {
              longArray[i] = RANDOM.nextLong();
            }
            LONG_ARRAYS[rowId] = longArray;
            dataTableBuilder.setColumn(colId, longArray);
            break;
          case FLOAT_ARRAY:
            length = RANDOM.nextInt(20);
            float[] floatArray = new float[length];
            for (int i = 0; i < length; i++) {
              floatArray[i] = RANDOM.nextFloat();
            }
            FLOAT_ARRAYS[rowId] = floatArray;
            dataTableBuilder.setColumn(colId, floatArray);
            break;
          case DOUBLE_ARRAY:
            length = RANDOM.nextInt(20);
            double[] doubleArray = new double[length];
            for (int i = 0; i < length; i++) {
              doubleArray[i] = RANDOM.nextDouble();
            }
            DOUBLE_ARRAYS[rowId] = doubleArray;
            dataTableBuilder.setColumn(colId, doubleArray);
            break;
          case BOOLEAN_ARRAY:
            length = RANDOM.nextInt(2);
            int[] booleanArray = new int[length];
            for (int i = 0; i < length; i++) {
              booleanArray[i] = RANDOM.nextInt();
            }
            BOOLEAN_ARRAYS[rowId] = booleanArray;
            dataTableBuilder.setColumn(colId, booleanArray);
            break;
          case TIMESTAMP_ARRAY:
            length = RANDOM.nextInt(20);
            long[] timestampArray = new long[length];
            for (int i = 0; i < length; i++) {
              timestampArray[i] = RANDOM.nextLong();
            }
            TIMESTAMP_ARRAYS[rowId] = timestampArray;
            dataTableBuilder.setColumn(colId, timestampArray);
            break;
          case BYTES_ARRAY:
            // TODO: add once implementation of datatable bytes array support is added
            break;
          case STRING_ARRAY:
            length = RANDOM.nextInt(20);
            String[] stringArray = new String[length];
            for (int i = 0; i < length; i++) {
              stringArray[i] = RandomStringUtils.random(RANDOM.nextInt(20));
            }
            STRING_ARRAYS[rowId] = stringArray;
            dataTableBuilder.setColumn(colId, stringArray);
            break;
          case MAP:
            Map<String, Object> map = new HashMap<>();
            for (int j = 0; j < 1 + RANDOM.nextInt(20); j++) {
              map.put("k" + j, RandomStringUtils.random(RANDOM.nextInt(20)));
            }
            MAPS[rowId] = map;
            dataTableBuilder.setColumn(colId, map);
            break;
          case UNKNOWN:
            dataTableBuilder.setColumn(colId, (Object) null);
            break;
          default:
            throw new UnsupportedOperationException("Unable to generate random data for: " + columnDataTypes[colId]);
        }
      }
      dataTableBuilder.finishRow();
    }
    if (nullBitmaps != null) {
      for (int colId = 0; colId < numColumns; colId++) {
        dataTableBuilder.setNullRowIds(nullBitmaps[colId]);
      }
    }
  }

  private void verifyDataIsSame(DataTable newDataTable, DataSchema.ColumnDataType[] columnDataTypes, int numColumns) {
    verifyDataIsSame(newDataTable, columnDataTypes, numColumns, NUM_ROWS);
  }

  private void verifyDataIsSame(DataTable newDataTable, DataSchema.ColumnDataType[] columnDataTypes, int numColumns,
      int numRows) {
    RoaringBitmap[] nullBitmaps = new RoaringBitmap[numColumns];
    for (int colId = 0; colId < numColumns; colId++) {
      nullBitmaps[colId] = newDataTable.getNullRowIds(colId);
    }
    for (int rowId = 0; rowId < numRows; rowId++) {
      for (int colId = 0; colId < numColumns; colId++) {
        boolean isNull = nullBitmaps[colId] != null && nullBitmaps[colId].contains(rowId);
        switch (columnDataTypes[colId]) {
          case INT:
            Assert.assertEquals(newDataTable.getInt(rowId, colId), isNull ? 0 : INTS[rowId], ERROR_MESSAGE);
            break;
          case LONG:
            Assert.assertEquals(newDataTable.getLong(rowId, colId), isNull ? 0 : LONGS[rowId], ERROR_MESSAGE);
            break;
          case FLOAT:
            Assert.assertEquals(newDataTable.getFloat(rowId, colId), isNull ? 0 : FLOATS[rowId], ERROR_MESSAGE);
            break;
          case DOUBLE:
            Assert.assertEquals(newDataTable.getDouble(rowId, colId), isNull ? 0.0 : DOUBLES[rowId], ERROR_MESSAGE);
            break;
          case BIG_DECIMAL:
            Assert.assertEquals(newDataTable.getBigDecimal(rowId, colId),
                isNull ? BigDecimal.ZERO : BIG_DECIMALS[rowId], ERROR_MESSAGE);
            break;
          case BOOLEAN:
            Assert.assertEquals(newDataTable.getInt(rowId, colId), isNull ? 0 : BOOLEANS[rowId], ERROR_MESSAGE);
            break;
          case TIMESTAMP:
            Assert.assertEquals(newDataTable.getLong(rowId, colId), isNull ? 0 : TIMESTAMPS[rowId], ERROR_MESSAGE);
            break;
          case STRING:
            Assert.assertEquals(newDataTable.getString(rowId, colId), isNull ? "" : STRINGS[rowId], ERROR_MESSAGE);
            break;
          case JSON:
            Assert.assertEquals(newDataTable.getString(rowId, colId), isNull ? "" : JSONS[rowId], ERROR_MESSAGE);
            break;
          case BYTES:
            Assert.assertEquals(newDataTable.getBytes(rowId, colId).getBytes(), isNull ? new byte[0] : BYTES[rowId],
                ERROR_MESSAGE);
            break;
          case OBJECT:
            CustomObject customObject = newDataTable.getCustomObject(rowId, colId);
            if (isNull) {
              Assert.assertNull(customObject, ERROR_MESSAGE);
            } else {
              Assert.assertNotNull(customObject);
              Assert.assertEquals(ObjectSerDeUtils.deserialize(customObject), OBJECTS[rowId], ERROR_MESSAGE);
            }
            break;
          case INT_ARRAY:
            Assert.assertTrue(Arrays.equals(newDataTable.getIntArray(rowId, colId), INT_ARRAYS[rowId]), ERROR_MESSAGE);
            break;
          case LONG_ARRAY:
            Assert.assertTrue(Arrays.equals(newDataTable.getLongArray(rowId, colId), LONG_ARRAYS[rowId]),
                ERROR_MESSAGE);
            break;
          case FLOAT_ARRAY:
            Assert.assertTrue(Arrays.equals(newDataTable.getFloatArray(rowId, colId), FLOAT_ARRAYS[rowId]),
                ERROR_MESSAGE);
            break;
          case DOUBLE_ARRAY:
            Assert.assertTrue(Arrays.equals(newDataTable.getDoubleArray(rowId, colId), DOUBLE_ARRAYS[rowId]),
                ERROR_MESSAGE);
            break;
          case BOOLEAN_ARRAY:
            Assert.assertTrue(Arrays.equals(newDataTable.getIntArray(rowId, colId), BOOLEAN_ARRAYS[rowId]),
                ERROR_MESSAGE);
            break;
          case TIMESTAMP_ARRAY:
            Assert.assertTrue(Arrays.equals(newDataTable.getLongArray(rowId, colId), TIMESTAMP_ARRAYS[rowId]),
                ERROR_MESSAGE);
            break;
          case BYTES_ARRAY:
            // TODO: add once implementation of datatable bytes array support is added
            break;
          case STRING_ARRAY:
            Assert.assertTrue(Arrays.equals(newDataTable.getStringArray(rowId, colId), STRING_ARRAYS[rowId]),
                ERROR_MESSAGE);
            break;
          case MAP:
            Assert.assertEquals(newDataTable.getMap(rowId, colId), MAPS[rowId], ERROR_MESSAGE);
            break;
          case UNKNOWN:
            Object nulValue = newDataTable.getCustomObject(rowId, colId);
            Assert.assertNull(nulValue, ERROR_MESSAGE);
            break;
          default:
            throw new UnsupportedOperationException("Unable to generate random data for: " + columnDataTypes[colId]);
        }
      }
    }
  }

  @DataProvider(name = "versionProvider")
  public Object[][] provideVersion() {
    return new Object[][]{
        new Object[]{DataTableFactory.VERSION_4}
    };
  }
}
