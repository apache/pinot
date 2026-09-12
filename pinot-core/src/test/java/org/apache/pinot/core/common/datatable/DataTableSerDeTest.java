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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.UuidUtils;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Unit test for [DataTable] serialization/de-serialization.
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
  private static final byte[][] UUIDS = new byte[NUM_ROWS][];
  private static final Object[] OBJECTS = new Object[NUM_ROWS];
  private static final int[][] INT_ARRAYS = new int[NUM_ROWS][];
  private static final long[][] LONG_ARRAYS = new long[NUM_ROWS][];
  private static final float[][] FLOAT_ARRAYS = new float[NUM_ROWS][];
  private static final double[][] DOUBLE_ARRAYS = new double[NUM_ROWS][];
  private static final int[][] BOOLEAN_ARRAYS = new int[NUM_ROWS][];
  private static final long[][] TIMESTAMP_ARRAYS = new long[NUM_ROWS][];
  private static final String[][] STRING_ARRAYS = new String[NUM_ROWS][];
  private static final ByteArray[][] BYTES_ARRAYS = new ByteArray[NUM_ROWS][];
  private static final ByteArray[][] UUID_ARRAYS = new ByteArray[NUM_ROWS][];
  private static final BigDecimal[][] BIG_DECIMAL_ARRAYS = new BigDecimal[NUM_ROWS][];
  private static final Map<String, Object>[] MAPS = new Map[NUM_ROWS];

  @Test(dataProvider = "versionProvider")
  public void testException(int dataTableVersion)
      throws IOException {
    DataTableBuilderFactory.setDataTableVersion(dataTableVersion);
    String expected = "Caught exception.";

    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.addException(QueryErrorCode.QUERY_EXECUTION, expected);
    DataTable newDataTable = DataTableFactory.getDataTable(dataTable.toBytes());
    assertNull(newDataTable.getDataSchema());
    assertEquals(newDataTable.getNumberOfRows(), 0);

    String actual = newDataTable.getExceptions().get(QueryErrorCode.QUERY_EXECUTION.getId());
    assertEquals(actual, expected);
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
          fail();
        }
      }
      dataTableBuilder.finishRow();
    }

    DataTable dataTable = dataTableBuilder.build();
    DataTable newDataTable = DataTableFactory.getDataTable(dataTable.toBytes());
    assertEquals(newDataTable.getDataSchema(), dataSchema);
    assertEquals(newDataTable.getNumberOfRows(), numRows);

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
        assertEquals(entry, emptyValues[columnId]);
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
    assertEquals(newDataTable.getDataSchema(), dataSchema, ERROR_MESSAGE);
    assertEquals(newDataTable.getNumberOfRows(), NUM_ROWS, ERROR_MESSAGE);
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
        assertEquals(newDataTable.getDataSchema(), dataSchema, ERROR_MESSAGE);
        assertEquals(newDataTable.getNumberOfRows(), numRows, ERROR_MESSAGE);
        verifyDataIsSame(newDataTable, columnDataType, 1, numRows);
      }
    }
  }

  /// [DataTableBuilder#setNull] must round-trip a null for every column type, not just the ones that can encode a
  /// null in-band. A type stored in a 4-byte slot (`INT` / `FLOAT` / `STRING` / `BOOLEAN`) is the sharpest case:
  /// writing the 8-byte custom-object encoding into it overflows the row buffer when the column is last, and
  /// silently overwrites the following column otherwise.
  @Test(dataProvider = "versionProvider")
  public void testSetNullForAllDataTypes(int dataTableVersion)
      throws IOException {
    DataTableBuilderFactory.setDataTableVersion(dataTableVersion);
    for (DataSchema.ColumnDataType columnDataType : DataSchema.ColumnDataType.values()) {
      DataSchema dataSchema =
          new DataSchema(new String[]{columnDataType.name()}, new DataSchema.ColumnDataType[]{columnDataType});
      DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
      dataTableBuilder.startRow();
      dataTableBuilder.setNull(0);
      dataTableBuilder.finishRow();
      DataTable dataTable = DataTableFactory.getDataTable(dataTableBuilder.build().toBytes());

      String message = ERROR_MESSAGE + ", type: " + columnDataType;
      assertEquals(dataTable.getNumberOfRows(), 1, message);
      switch (columnDataType.getStoredType()) {
        // Types that carry their own in-band null encoding need no bitmap entry.
        case OBJECT:
        case UNKNOWN:
          assertNull(dataTable.getCustomObject(0, 0), message);
          assertNull(dataTable.getNullRowIds(0), message);
          break;
        case MAP:
          assertNull(dataTable.getMap(0, 0), message);
          assertNull(dataTable.getNullRowIds(0), message);
          break;
        default:
          RoaringBitmap nullRowIds = dataTable.getNullRowIds(0);
          assertNotNull(nullRowIds, message);
          assertTrue(nullRowIds.contains(0), message);
          break;
      }
    }
  }

  /// A null in a 4-byte column must not bleed into the column that follows it.
  @Test(dataProvider = "versionProvider")
  public void testSetNullDoesNotCorruptAdjacentColumn(int dataTableVersion)
      throws IOException {
    DataTableBuilderFactory.setDataTableVersion(dataTableVersion);
    DataSchema dataSchema = new DataSchema(new String[]{"str", "dbl", "int"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.INT
    });
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    dataTableBuilder.startRow();
    dataTableBuilder.setNull(0);
    dataTableBuilder.setColumn(1, 3.5d);
    dataTableBuilder.setColumn(2, 42);
    dataTableBuilder.finishRow();
    DataTable dataTable = DataTableFactory.getDataTable(dataTableBuilder.build().toBytes());

    assertEquals(dataTable.getDouble(0, 1), 3.5d, ERROR_MESSAGE);
    assertEquals(dataTable.getInt(0, 2), 42, ERROR_MESSAGE);
    assertEquals(dataTable.getNullRowIds(0), RoaringBitmap.bitmapOf(0), ERROR_MESSAGE);
    assertNull(dataTable.getNullRowIds(1), ERROR_MESSAGE);
    assertNull(dataTable.getNullRowIds(2), ERROR_MESSAGE);
  }

  /// A table without nulls must not grow a null bitmap section, so that its bytes stay identical to what a writer
  /// that predates always-on null support would have produced.
  @Test(dataProvider = "versionProvider")
  public void testNoNullBitmapSectionWithoutNulls(int dataTableVersion)
      throws IOException {
    DataTableBuilderFactory.setDataTableVersion(dataTableVersion);
    DataSchema dataSchema = new DataSchema(new String[]{"str", "int"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT});
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    dataTableBuilder.startRow();
    dataTableBuilder.setColumn(0, "foo");
    dataTableBuilder.setColumn(1, 42);
    dataTableBuilder.finishRow();
    // An all-empty bitmap carries no information and must not force the section to be emitted either.
    dataTableBuilder.setNullRowIds(new RoaringBitmap());
    dataTableBuilder.setNullRowIds(null);
    byte[] bytes = dataTableBuilder.build().toBytes();

    DataTableBuilder controlBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    controlBuilder.startRow();
    controlBuilder.setColumn(0, "foo");
    controlBuilder.setColumn(1, 42);
    controlBuilder.finishRow();
    assertEquals(bytes, controlBuilder.build().toBytes(), ERROR_MESSAGE);

    DataTable dataTable = DataTableFactory.getDataTable(bytes);
    assertNull(dataTable.getNullRowIds(0), ERROR_MESSAGE);
    assertNull(dataTable.getNullRowIds(1), ERROR_MESSAGE);
  }

  /// A pre-computed bitmap handed to [DataTableBuilder#setNullRowIds] must merge with the per-cell nulls recorded
  /// by [DataTableBuilder#setNull] for the same column rather than replacing them.
  @Test(dataProvider = "versionProvider")
  public void testSetNullRowIdsMergesWithSetNull(int dataTableVersion)
      throws IOException {
    DataTableBuilderFactory.setDataTableVersion(dataTableVersion);
    DataSchema dataSchema =
        new DataSchema(new String[]{"int"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    for (int rowId = 0; rowId < 3; rowId++) {
      dataTableBuilder.startRow();
      if (rowId == 0) {
        dataTableBuilder.setNull(0);
      } else {
        dataTableBuilder.setColumn(0, rowId);
      }
      dataTableBuilder.finishRow();
    }
    RoaringBitmap preComputed = RoaringBitmap.bitmapOf(2);
    dataTableBuilder.setNullRowIds(preComputed);
    DataTable dataTable = DataTableFactory.getDataTable(dataTableBuilder.build().toBytes());

    assertEquals(dataTable.getNullRowIds(0), RoaringBitmap.bitmapOf(0, 2), ERROR_MESSAGE);
    assertEquals(dataTable.getInt(1, 0), 1, ERROR_MESSAGE);
    // The builder must not take ownership of the caller's bitmap.
    assertEquals(preComputed, RoaringBitmap.bitmapOf(2), ERROR_MESSAGE);
  }

  @Test(dataProvider = "versionProvider")
  public void testThreadCPUMemMeasurement(int dataTableVersion)
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

    // Enable ThreadCpuTimeMeasurement, serialize/de-serialize data table.
    ThreadResourceUsageProvider.setThreadCpuTimeMeasurementEnabled(true);
    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(true);
    DataTable dataTable = dataTableBuilder.build();
    DataTable newDataTable = DataTableFactory.getDataTable(dataTable.toBytes());
    // When ThreadCpuTimeMeasurement is enabled, responseSerializationCpuTimeNs should be positive.
    assertNull(newDataTable.getMetadata().get(MetadataKey.THREAD_CPU_TIME_NS.getName()));
    assertNull(newDataTable.getMetadata().get(MetadataKey.SYSTEM_ACTIVITIES_CPU_TIME_NS.getName()));
    assertNull(newDataTable.getMetadata().get(MetadataKey.THREAD_MEM_ALLOCATED_BYTES.getName()));
    assertTrue(Integer.parseInt(newDataTable.getMetadata().get(MetadataKey.RESPONSE_SER_CPU_TIME_NS.getName())) > 0);
    assertTrue(
        Integer.parseInt(newDataTable.getMetadata().get(MetadataKey.RESPONSE_SER_MEM_ALLOCATED_BYTES.getName())) > 0);

    // Disable ThreadCpuTimeMeasurement, serialize/de-serialize data table.
    ThreadResourceUsageProvider.setThreadCpuTimeMeasurementEnabled(false);
    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(false);
    dataTable = dataTableBuilder.build();
    newDataTable = DataTableFactory.getDataTable(dataTable.toBytes());
    // When measurement is disabled, response serialization metadata is absent.
    assertNull(newDataTable.getMetadata().get(MetadataKey.THREAD_CPU_TIME_NS.getName()));
    assertNull(newDataTable.getMetadata().get(MetadataKey.SYSTEM_ACTIVITIES_CPU_TIME_NS.getName()));
    assertNull(newDataTable.getMetadata().get(MetadataKey.RESPONSE_SER_CPU_TIME_NS.getName()));
    assertNull(newDataTable.getMetadata().get(MetadataKey.THREAD_MEM_ALLOCATED_BYTES.getName()));
    assertNull(newDataTable.getMetadata().get(MetadataKey.RESPONSE_SER_MEM_ALLOCATED_BYTES.getName()));
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
            STRINGS[rowId] = isNull ? "" : RandomStringUtils.secure().next(RANDOM.nextInt(20));
            dataTableBuilder.setColumn(colId, STRINGS[rowId]);
            break;
          case JSON:
            JSONS[rowId] = isNull ? "" : "{\"key\": \"" + RandomStringUtils.secure().next(RANDOM.nextInt(20)) + "\"}";
            dataTableBuilder.setColumn(colId, JSONS[rowId]);
            break;
          case BYTES:
            BYTES[rowId] = isNull ? new byte[0] : RandomStringUtils.secure().next(RANDOM.nextInt(20)).getBytes();
            dataTableBuilder.setColumn(colId, new ByteArray(BYTES[rowId]));
            break;
          case UUID:
            UUIDS[rowId] =
                isNull ? UuidUtils.nullUuidBytes() : UuidUtils.toBytes(new UUID(RANDOM.nextLong(), RANDOM.nextLong()));
            dataTableBuilder.setColumn(colId, new ByteArray(UUIDS[rowId]));
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
          case BIG_DECIMAL_ARRAY:
            length = RANDOM.nextInt(20);
            BigDecimal[] bigDecimalArray = new BigDecimal[length];
            for (int i = 0; i < length; i++) {
              bigDecimalArray[i] = BigDecimal.valueOf(RANDOM.nextDouble());
            }
            BIG_DECIMAL_ARRAYS[rowId] = bigDecimalArray;
            dataTableBuilder.setColumn(colId, bigDecimalArray);
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
            length = RANDOM.nextInt(20);
            ByteArray[] bytesArray = new ByteArray[length];
            for (int i = 0; i < length; i++) {
              bytesArray[i] =
                  new ByteArray(RandomStringUtils.secure().next(RANDOM.nextInt(20)).getBytes(StandardCharsets.UTF_8));
            }
            BYTES_ARRAYS[rowId] = bytesArray;
            dataTableBuilder.setColumn(colId, bytesArray);
            break;
          case UUID_ARRAY:
            length = RANDOM.nextInt(20);
            ByteArray[] uuidArray = new ByteArray[length];
            for (int i = 0; i < length; i++) {
              uuidArray[i] = new ByteArray(UuidUtils.toBytes(new UUID(RANDOM.nextLong(), RANDOM.nextLong())));
            }
            UUID_ARRAYS[rowId] = uuidArray;
            dataTableBuilder.setColumn(colId, uuidArray);
            break;
          case STRING_ARRAY:
            length = RANDOM.nextInt(20);
            String[] stringArray = new String[length];
            for (int i = 0; i < length; i++) {
              stringArray[i] = RandomStringUtils.secure().next(RANDOM.nextInt(20));
            }
            STRING_ARRAYS[rowId] = stringArray;
            dataTableBuilder.setColumn(colId, stringArray);
            break;
          case MAP:
            Map<String, Object> map = new HashMap<>();
            for (int j = 0; j < 1 + RANDOM.nextInt(20); j++) {
              map.put("k" + j, RandomStringUtils.secure().next(RANDOM.nextInt(20)));
            }
            MAPS[rowId] = map;
            dataTableBuilder.setColumn(colId, map);
            break;
          case OBJECT:
          case UNKNOWN:
            dataTableBuilder.setNull(colId);
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
            assertEquals(newDataTable.getInt(rowId, colId), isNull ? 0 : INTS[rowId], ERROR_MESSAGE);
            break;
          case LONG:
            assertEquals(newDataTable.getLong(rowId, colId), isNull ? 0 : LONGS[rowId], ERROR_MESSAGE);
            break;
          case FLOAT:
            assertEquals(newDataTable.getFloat(rowId, colId), isNull ? 0 : FLOATS[rowId], ERROR_MESSAGE);
            break;
          case DOUBLE:
            assertEquals(newDataTable.getDouble(rowId, colId), isNull ? 0.0 : DOUBLES[rowId], ERROR_MESSAGE);
            break;
          case BIG_DECIMAL:
            assertEquals(newDataTable.getBigDecimal(rowId, colId), isNull ? BigDecimal.ZERO : BIG_DECIMALS[rowId],
                ERROR_MESSAGE);
            break;
          case BOOLEAN:
            assertEquals(newDataTable.getInt(rowId, colId), isNull ? 0 : BOOLEANS[rowId], ERROR_MESSAGE);
            break;
          case TIMESTAMP:
            assertEquals(newDataTable.getLong(rowId, colId), isNull ? 0 : TIMESTAMPS[rowId], ERROR_MESSAGE);
            break;
          case STRING:
            assertEquals(newDataTable.getString(rowId, colId), isNull ? "" : STRINGS[rowId], ERROR_MESSAGE);
            break;
          case JSON:
            assertEquals(newDataTable.getString(rowId, colId), isNull ? "" : JSONS[rowId], ERROR_MESSAGE);
            break;
          case BYTES:
            assertEquals(newDataTable.getBytes(rowId, colId).getBytes(), isNull ? new byte[0] : BYTES[rowId],
                ERROR_MESSAGE);
            break;
          case UUID:
            assertEquals(newDataTable.getBytes(rowId, colId).getBytes(),
                isNull ? UuidUtils.nullUuidBytes() : UUIDS[rowId], ERROR_MESSAGE);
            break;
          case INT_ARRAY:
            assertTrue(Arrays.equals(newDataTable.getIntArray(rowId, colId), INT_ARRAYS[rowId]), ERROR_MESSAGE);
            break;
          case LONG_ARRAY:
            assertTrue(Arrays.equals(newDataTable.getLongArray(rowId, colId), LONG_ARRAYS[rowId]), ERROR_MESSAGE);
            break;
          case FLOAT_ARRAY:
            assertTrue(Arrays.equals(newDataTable.getFloatArray(rowId, colId), FLOAT_ARRAYS[rowId]), ERROR_MESSAGE);
            break;
          case DOUBLE_ARRAY:
            assertTrue(Arrays.equals(newDataTable.getDoubleArray(rowId, colId), DOUBLE_ARRAYS[rowId]), ERROR_MESSAGE);
            break;
          case BIG_DECIMAL_ARRAY:
            assertTrue(Arrays.equals(newDataTable.getBigDecimalArray(rowId, colId), BIG_DECIMAL_ARRAYS[rowId]),
                ERROR_MESSAGE);
            break;
          case BOOLEAN_ARRAY:
            assertTrue(Arrays.equals(newDataTable.getIntArray(rowId, colId), BOOLEAN_ARRAYS[rowId]), ERROR_MESSAGE);
            break;
          case TIMESTAMP_ARRAY:
            assertTrue(Arrays.equals(newDataTable.getLongArray(rowId, colId), TIMESTAMP_ARRAYS[rowId]), ERROR_MESSAGE);
            break;
          case BYTES_ARRAY:
            assertTrue(Arrays.equals(newDataTable.getBytesArray(rowId, colId), BYTES_ARRAYS[rowId]), ERROR_MESSAGE);
            break;
          case UUID_ARRAY:
            assertTrue(Arrays.equals(newDataTable.getBytesArray(rowId, colId), UUID_ARRAYS[rowId]), ERROR_MESSAGE);
            break;
          case STRING_ARRAY:
            assertTrue(Arrays.equals(newDataTable.getStringArray(rowId, colId), STRING_ARRAYS[rowId]), ERROR_MESSAGE);
            break;
          case MAP:
            assertEquals(newDataTable.getMap(rowId, colId), MAPS[rowId], ERROR_MESSAGE);
            break;
          case OBJECT:
          case UNKNOWN:
            Object nulValue = newDataTable.getCustomObject(rowId, colId);
            assertNull(nulValue, ERROR_MESSAGE);
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
