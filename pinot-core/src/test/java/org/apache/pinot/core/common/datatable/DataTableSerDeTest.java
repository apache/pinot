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

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.request.context.ThreadTimer;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;


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
  private static final Map<String, String> EXPECTED_METADATA =
      ImmutableMap.<String, String>builder().put(MetadataKey.NUM_DOCS_SCANNED.getName(), String.valueOf(20L))
          .put(MetadataKey.NUM_ENTRIES_SCANNED_IN_FILTER.getName(), String.valueOf(5L))
          .put(MetadataKey.NUM_ENTRIES_SCANNED_POST_FILTER.getName(), String.valueOf(7L))
          .put(MetadataKey.NUM_SEGMENTS_QUERIED.getName(), String.valueOf(6))
          .put(MetadataKey.NUM_SEGMENTS_PROCESSED.getName(), String.valueOf(6))
          .put(MetadataKey.NUM_SEGMENTS_MATCHED.getName(), String.valueOf(1))
          .put(MetadataKey.NUM_CONSUMING_SEGMENTS_QUERIED.getName(), String.valueOf(1))
          .put(MetadataKey.MIN_CONSUMING_FRESHNESS_TIME_MS.getName(), String.valueOf(100L))
          .put(MetadataKey.TOTAL_DOCS.getName(), String.valueOf(200L))
          .put(MetadataKey.NUM_GROUPS_LIMIT_REACHED.getName(), "true")
          .put(MetadataKey.TIME_USED_MS.getName(), String.valueOf(20000L)).put(MetadataKey.TRACE_INFO.getName(),
              "StudentException: Error finding students\n"
                  + "        at StudentManager.findStudents(StudentManager.java:13)\n"
                  + "        at StudentProgram.main(StudentProgram.java:9)\n"
                  + "Caused by: DAOException: Error querying students from database\n"
                  + "        at StudentDAO.list(StudentDAO.java:11)\n"
                  + "        at StudentManager.findStudents(StudentManager.java:11)\n" + "        ... 1 more\n"
                  + "Caused by: java.sql.SQLException: Syntax Error\n"
                  + "        at DatabaseUtils.executeQuery(DatabaseUtils.java:5)\n"
                  + "        at StudentDAO.list(StudentDAO.java:8)\n" + "        ... 2 more")
          .put(MetadataKey.REQUEST_ID.getName(), String.valueOf(90181881818L))
          .put(MetadataKey.NUM_RESIZES.getName(), String.valueOf(900L))
          .put(MetadataKey.RESIZE_TIME_MS.getName(), String.valueOf(1919199L)).build();

  @Test
  public void testException()
      throws IOException {
    Exception exception = new UnsupportedOperationException("Caught exception.");
    ProcessingException processingException =
        QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, exception);
    String expected = processingException.getMessage();

    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.addException(processingException);
    DataTable newDataTable = DataTableFactory.getDataTable(dataTable.toBytes());
    Assert.assertNull(newDataTable.getDataSchema());
    Assert.assertEquals(newDataTable.getNumberOfRows(), 0);

    String actual = newDataTable.getExceptions().get(QueryException.QUERY_EXECUTION_ERROR.getErrorCode());
    Assert.assertEquals(actual, expected);
  }

  @Test
  public void testEmptyStrings()
      throws IOException {
    String emptyString = StringUtils.EMPTY;
    String[] emptyStringArray = {StringUtils.EMPTY};

    DataSchema dataSchema = new DataSchema(new String[]{"SV", "MV"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING_ARRAY});
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
      dataTableBuilder.startRow();
      dataTableBuilder.setColumn(0, emptyString);
      dataTableBuilder.setColumn(1, emptyStringArray);
      dataTableBuilder.finishRow();
    }

    DataTable dataTable = dataTableBuilder.build();
    DataTable newDataTable = DataTableFactory.getDataTable(dataTable.toBytes());
    Assert.assertEquals(newDataTable.getDataSchema(), dataSchema);
    Assert.assertEquals(newDataTable.getNumberOfRows(), NUM_ROWS);

    for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
      Assert.assertEquals(newDataTable.getString(rowId, 0), emptyString);
      Assert.assertEquals(newDataTable.getStringArray(rowId, 1), emptyStringArray);
    }
  }

  @Test
  public void testAllDataTypes()
      throws IOException {
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

  @Test
  public void testV3V4Compatibility()
      throws IOException {
    DataSchema.ColumnDataType[] columnDataTypes = DataSchema.ColumnDataType.values();
    int numColumns = columnDataTypes.length;
    String[] columnNames = new String[numColumns];
    for (int i = 0; i < numColumns; i++) {
      columnNames[i] = columnDataTypes[i].name();
    }

    DataSchema dataSchema = new DataSchema(columnNames, columnDataTypes);

    // TODO: verify data table compatibility across multi-stage and normal query engine.
    // TODO: see https://github.com/apache/pinot/pull/8874/files#r894806085

    // Verify V4 broker can deserialize data table (has data, but has no metadata) send by V3 server
    ThreadTimer.setThreadCpuTimeMeasurementEnabled(false);
    DataTableBuilderFactory.setDataTableVersion(DataTableFactory.VERSION_3);
    DataTableBuilder dataTableBuilderV3WithDataOnly = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    fillDataTableWithRandomData(dataTableBuilderV3WithDataOnly, columnDataTypes, numColumns);

    DataTable dataTableV3 = dataTableBuilderV3WithDataOnly.build(); // create a V3 data table
    DataTable newDataTable =
        DataTableFactory.getDataTable(dataTableV3.toBytes()); // Broker deserialize data table bytes as V3
    Assert.assertEquals(newDataTable.getDataSchema(), dataSchema, ERROR_MESSAGE);
    Assert.assertEquals(newDataTable.getNumberOfRows(), NUM_ROWS, ERROR_MESSAGE);
    verifyDataIsSame(newDataTable, columnDataTypes, numColumns);
    Assert.assertEquals(newDataTable.getMetadata().size(), 0);

    // Verify V4 broker can deserialize data table (has data and metadata) send by V3 server
    for (String key : EXPECTED_METADATA.keySet()) {
      dataTableV3.getMetadata().put(key, EXPECTED_METADATA.get(key));
    }
    newDataTable = DataTableFactory.getDataTable(dataTableV3.toBytes()); // Broker deserialize data table bytes as V3
    Assert.assertEquals(newDataTable.getDataSchema(), dataSchema, ERROR_MESSAGE);
    Assert.assertEquals(newDataTable.getNumberOfRows(), NUM_ROWS, ERROR_MESSAGE);
    verifyDataIsSame(newDataTable, columnDataTypes, numColumns);
    Assert.assertEquals(newDataTable.getMetadata(), EXPECTED_METADATA);

    // Verify V4 broker can deserialize data table (only has metadata) send by V3 server
    DataTableBuilder dataTableBuilderV3WithMetadataDataOnly = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    dataTableV3 = dataTableBuilderV3WithMetadataDataOnly.build(); // create a V3 data table
    for (String key : EXPECTED_METADATA.keySet()) {
      dataTableV3.getMetadata().put(key, EXPECTED_METADATA.get(key));
    }
    newDataTable = DataTableFactory.getDataTable(dataTableV3.toBytes()); // Broker deserialize data table bytes as V3
    Assert.assertEquals(newDataTable.getDataSchema(), dataSchema, ERROR_MESSAGE);
    Assert.assertEquals(newDataTable.getNumberOfRows(), 0, 0);
    Assert.assertEquals(newDataTable.getMetadata(), EXPECTED_METADATA);

    // Verify V4 broker can deserialize (has data, but has no metadata) send by V4 server(with ThreadCpuTimeMeasurement
    // disabled)
    ThreadTimer.setThreadCpuTimeMeasurementEnabled(false);
    DataTableBuilderFactory.setDataTableVersion(DataTableFactory.VERSION_4);
    DataTableBuilder dataTableBuilderV4WithDataOnly = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    fillDataTableWithRandomData(dataTableBuilderV4WithDataOnly, columnDataTypes, numColumns);
    DataTable dataTableV4 = dataTableBuilderV4WithDataOnly.build(); // create a V4 data table
    // Deserialize data table bytes as V4
    newDataTable = DataTableFactory.getDataTable(dataTableV4.toBytes());
    Assert.assertEquals(newDataTable.getDataSchema(), dataSchema, ERROR_MESSAGE);
    Assert.assertEquals(newDataTable.getNumberOfRows(), NUM_ROWS, ERROR_MESSAGE);
    verifyDataIsSame(newDataTable, columnDataTypes, numColumns);
    Assert.assertEquals(newDataTable.getMetadata().size(), 0);

    // Verify V4 broker can deserialize data table (has data and metadata) send by V4 server(with
    // ThreadCpuTimeMeasurement disabled)
    for (String key : EXPECTED_METADATA.keySet()) {
      dataTableV4.getMetadata().put(key, EXPECTED_METADATA.get(key));
    }
    // Deserialize data table bytes as V4
    newDataTable = DataTableFactory.getDataTable(dataTableV4.toBytes()); // Broker deserialize data table bytes as V4
    Assert.assertEquals(newDataTable.getDataSchema(), dataSchema, ERROR_MESSAGE);
    Assert.assertEquals(newDataTable.getNumberOfRows(), NUM_ROWS, ERROR_MESSAGE);
    verifyDataIsSame(newDataTable, columnDataTypes, numColumns);
    Assert.assertEquals(newDataTable.getMetadata(), EXPECTED_METADATA);

    // Verify V4 broker can deserialize data table (only has metadata) send by V4 server(with
    // ThreadCpuTimeMeasurement disabled)
    DataTableBuilder dataTableBuilderV4WithMetadataDataOnly = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    dataTableV4 = dataTableBuilderV4WithMetadataDataOnly.build(); // create a V4 data table
    for (String key : EXPECTED_METADATA.keySet()) {
      dataTableV4.getMetadata().put(key, EXPECTED_METADATA.get(key));
    }
    newDataTable = DataTableFactory.getDataTable(dataTableV4.toBytes()); // Broker deserialize data table bytes as V4
    Assert.assertEquals(newDataTable.getDataSchema(), dataSchema, ERROR_MESSAGE);
    Assert.assertEquals(newDataTable.getNumberOfRows(), 0, 0);
    Assert.assertEquals(newDataTable.getMetadata(), EXPECTED_METADATA);

    // Verify V4 broker can deserialize (has data, but has no metadata) send by V4 server(with ThreadCpuTimeMeasurement
    // enabled)
    ThreadTimer.setThreadCpuTimeMeasurementEnabled(true);
    DataTableBuilderFactory.setDataTableVersion(DataTableFactory.VERSION_4);
    dataTableV4 = dataTableBuilderV4WithDataOnly.build(); // create a V4 data table
    // Deserialize data table bytes as V4
    newDataTable = DataTableFactory.getDataTable(dataTableV4.toBytes());
    Assert.assertEquals(newDataTable.getDataSchema(), dataSchema, ERROR_MESSAGE);
    Assert.assertEquals(newDataTable.getNumberOfRows(), NUM_ROWS, ERROR_MESSAGE);
    verifyDataIsSame(newDataTable, columnDataTypes, numColumns);
    Assert.assertEquals(newDataTable.getMetadata().size(), 1);
    Assert.assertTrue(newDataTable.getMetadata().containsKey(MetadataKey.RESPONSE_SER_CPU_TIME_NS.getName()));

    // Verify V4 broker can deserialize data table (has data and metadata) send by V4 server(with
    // ThreadCpuTimeMeasurement enabled)
    for (String key : EXPECTED_METADATA.keySet()) {
      dataTableV4.getMetadata().put(key, EXPECTED_METADATA.get(key));
    }
    // Deserialize data table bytes as V4
    newDataTable = DataTableFactory.getDataTable(dataTableV4.toBytes()); // Broker deserialize data table bytes as V4
    Assert.assertEquals(newDataTable.getDataSchema(), dataSchema, ERROR_MESSAGE);
    Assert.assertEquals(newDataTable.getNumberOfRows(), NUM_ROWS, ERROR_MESSAGE);
    verifyDataIsSame(newDataTable, columnDataTypes, numColumns);
    if (ThreadTimer.isThreadCpuTimeMeasurementEnabled()) {
      Assert.assertEquals(newDataTable.getMetadata().size(), EXPECTED_METADATA.keySet().size() + 1);
      newDataTable.getMetadata().remove(MetadataKey.RESPONSE_SER_CPU_TIME_NS.getName());
    }
    Assert.assertEquals(newDataTable.getMetadata(), EXPECTED_METADATA);

    // Verify V4 broker can deserialize data table (only has metadata) send by V4 server(with
    // ThreadCpuTimeMeasurement enabled)
    dataTableV4 = dataTableBuilderV4WithMetadataDataOnly.build(); // create a V4 data table
    for (String key : EXPECTED_METADATA.keySet()) {
      dataTableV4.getMetadata().put(key, EXPECTED_METADATA.get(key));
    }
    newDataTable = DataTableFactory.getDataTable(dataTableV4.toBytes()); // Broker deserialize data table bytes as V4
    Assert.assertEquals(newDataTable.getDataSchema(), dataSchema, ERROR_MESSAGE);
    Assert.assertEquals(newDataTable.getNumberOfRows(), 0, 0);
    if (ThreadTimer.isThreadCpuTimeMeasurementEnabled()) {
      Assert.assertEquals(newDataTable.getMetadata().size(), EXPECTED_METADATA.keySet().size() + 1);
      newDataTable.getMetadata().remove(MetadataKey.RESPONSE_SER_CPU_TIME_NS.getName());
    }
    Assert.assertEquals(newDataTable.getMetadata(), EXPECTED_METADATA);
  }

  @Test
  public void testV2V3Compatibility()
      throws IOException {
    DataSchema.ColumnDataType[] columnDataTypes = DataSchema.ColumnDataType.values();
    int numColumns = columnDataTypes.length;
    String[] columnNames = new String[numColumns];
    for (int i = 0; i < numColumns; i++) {
      columnNames[i] = columnDataTypes[i].name();
    }

    DataSchema dataSchema = new DataSchema(columnNames, columnDataTypes);

    // Verify V3 broker can deserialize data table (has data, but has no metadata) send by V2 server
    ThreadTimer.setThreadCpuTimeMeasurementEnabled(false);
    DataTableBuilderFactory.setDataTableVersion(DataTableFactory.VERSION_2);
    DataTableBuilder dataTableBuilderV2WithDataOnly = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    fillDataTableWithRandomData(dataTableBuilderV2WithDataOnly, columnDataTypes, numColumns);

    DataTable dataTableV2 = dataTableBuilderV2WithDataOnly.build(); // create a V2 data table
    DataTable newDataTable =
        DataTableFactory.getDataTable(dataTableV2.toBytes()); // Broker deserialize data table bytes as V2
    Assert.assertEquals(newDataTable.getDataSchema(), dataSchema, ERROR_MESSAGE);
    Assert.assertEquals(newDataTable.getNumberOfRows(), NUM_ROWS, ERROR_MESSAGE);
    verifyDataIsSame(newDataTable, columnDataTypes, numColumns);
    Assert.assertEquals(newDataTable.getMetadata().size(), 0);

    // Verify V3 broker can deserialize data table (has data and metadata) send by V2 server
    for (String key : EXPECTED_METADATA.keySet()) {
      dataTableV2.getMetadata().put(key, EXPECTED_METADATA.get(key));
    }
    newDataTable = DataTableFactory.getDataTable(dataTableV2.toBytes()); // Broker deserialize data table bytes as V2
    Assert.assertEquals(newDataTable.getDataSchema(), dataSchema, ERROR_MESSAGE);
    Assert.assertEquals(newDataTable.getNumberOfRows(), NUM_ROWS, ERROR_MESSAGE);
    verifyDataIsSame(newDataTable, columnDataTypes, numColumns);
    Assert.assertEquals(newDataTable.getMetadata(), EXPECTED_METADATA);

    // Verify V3 broker can deserialize data table (only has metadata) send by V2 server
    DataTableBuilder dataTableBuilderV2WithMetadataDataOnly = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    dataTableV2 = dataTableBuilderV2WithMetadataDataOnly.build(); // create a V2 data table
    for (String key : EXPECTED_METADATA.keySet()) {
      dataTableV2.getMetadata().put(key, EXPECTED_METADATA.get(key));
    }
    newDataTable = DataTableFactory.getDataTable(dataTableV2.toBytes()); // Broker deserialize data table bytes as V2
    Assert.assertEquals(newDataTable.getDataSchema(), dataSchema, ERROR_MESSAGE);
    Assert.assertEquals(newDataTable.getNumberOfRows(), 0, 0);
    Assert.assertEquals(newDataTable.getMetadata(), EXPECTED_METADATA);

    // Verify V3 broker can deserialize (has data, but has no metadata) send by V3 server(with ThreadCpuTimeMeasurement
    // disabled)
    ThreadTimer.setThreadCpuTimeMeasurementEnabled(false);
    DataTableBuilderFactory.setDataTableVersion(DataTableFactory.VERSION_3);
    DataTableBuilder dataTableBuilderV3WithDataOnly = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    fillDataTableWithRandomData(dataTableBuilderV3WithDataOnly, columnDataTypes, numColumns);
    DataTable dataTableV3 = dataTableBuilderV3WithDataOnly.build(); // create a V3 data table
    // Deserialize data table bytes as V3
    newDataTable = DataTableFactory.getDataTable(dataTableV3.toBytes());
    Assert.assertEquals(newDataTable.getDataSchema(), dataSchema, ERROR_MESSAGE);
    Assert.assertEquals(newDataTable.getNumberOfRows(), NUM_ROWS, ERROR_MESSAGE);
    verifyDataIsSame(newDataTable, columnDataTypes, numColumns);
    Assert.assertEquals(newDataTable.getMetadata().size(), 0);

    // Verify V3 broker can deserialize data table (has data and metadata) send by V3 server(with
    // ThreadCpuTimeMeasurement disabled)
    for (String key : EXPECTED_METADATA.keySet()) {
      dataTableV3.getMetadata().put(key, EXPECTED_METADATA.get(key));
    }
    // Deserialize data table bytes as V3
    newDataTable = DataTableFactory.getDataTable(dataTableV3.toBytes()); // Broker deserialize data table bytes as V3
    Assert.assertEquals(newDataTable.getDataSchema(), dataSchema, ERROR_MESSAGE);
    Assert.assertEquals(newDataTable.getNumberOfRows(), NUM_ROWS, ERROR_MESSAGE);
    verifyDataIsSame(newDataTable, columnDataTypes, numColumns);
    Assert.assertEquals(newDataTable.getMetadata(), EXPECTED_METADATA);

    // Verify V3 broker can deserialize data table (only has metadata) send by V3 server(with
    // ThreadCpuTimeMeasurement disabled)
    DataTableBuilder dataTableBuilderV3WithMetadataDataOnly = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    dataTableV3 = dataTableBuilderV3WithMetadataDataOnly.build(); // create a V3 data table
    for (String key : EXPECTED_METADATA.keySet()) {
      dataTableV3.getMetadata().put(key, EXPECTED_METADATA.get(key));
    }
    newDataTable = DataTableFactory.getDataTable(dataTableV3.toBytes()); // Broker deserialize data table bytes as V3
    Assert.assertEquals(newDataTable.getDataSchema(), dataSchema, ERROR_MESSAGE);
    Assert.assertEquals(newDataTable.getNumberOfRows(), 0, 0);
    Assert.assertEquals(newDataTable.getMetadata(), EXPECTED_METADATA);

    // Verify V3 broker can deserialize (has data, but has no metadata) send by V3 server(with ThreadCpuTimeMeasurement
    // enabled)
    ThreadTimer.setThreadCpuTimeMeasurementEnabled(true);
    DataTableBuilderFactory.setDataTableVersion(DataTableFactory.VERSION_3);
    dataTableV3 = dataTableBuilderV3WithDataOnly.build(); // create a V3 data table
    // Deserialize data table bytes as V3
    newDataTable = DataTableFactory.getDataTable(dataTableV3.toBytes());
    Assert.assertEquals(newDataTable.getDataSchema(), dataSchema, ERROR_MESSAGE);
    Assert.assertEquals(newDataTable.getNumberOfRows(), NUM_ROWS, ERROR_MESSAGE);
    verifyDataIsSame(newDataTable, columnDataTypes, numColumns);
    Assert.assertEquals(newDataTable.getMetadata().size(), 1);
    Assert.assertTrue(newDataTable.getMetadata().containsKey(MetadataKey.RESPONSE_SER_CPU_TIME_NS.getName()));

    // Verify V3 broker can deserialize data table (has data and metadata) send by V3 server(with
    // ThreadCpuTimeMeasurement enabled)
    for (String key : EXPECTED_METADATA.keySet()) {
      dataTableV3.getMetadata().put(key, EXPECTED_METADATA.get(key));
    }
    // Deserialize data table bytes as V3
    newDataTable = DataTableFactory.getDataTable(dataTableV3.toBytes()); // Broker deserialize data table bytes as V3
    Assert.assertEquals(newDataTable.getDataSchema(), dataSchema, ERROR_MESSAGE);
    Assert.assertEquals(newDataTable.getNumberOfRows(), NUM_ROWS, ERROR_MESSAGE);
    verifyDataIsSame(newDataTable, columnDataTypes, numColumns);
    if (ThreadTimer.isThreadCpuTimeMeasurementEnabled()) {
      Assert.assertEquals(newDataTable.getMetadata().size(), EXPECTED_METADATA.keySet().size() + 1);
      newDataTable.getMetadata().remove(MetadataKey.RESPONSE_SER_CPU_TIME_NS.getName());
    }
    Assert.assertEquals(newDataTable.getMetadata(), EXPECTED_METADATA);

    // Verify V3 broker can deserialize data table (only has metadata) send by V3 server(with
    // ThreadCpuTimeMeasurement enabled)
    dataTableV3 = dataTableBuilderV3WithMetadataDataOnly.build(); // create a V3 data table
    for (String key : EXPECTED_METADATA.keySet()) {
      dataTableV3.getMetadata().put(key, EXPECTED_METADATA.get(key));
    }
    newDataTable = DataTableFactory.getDataTable(dataTableV3.toBytes()); // Broker deserialize data table bytes as V3
    Assert.assertEquals(newDataTable.getDataSchema(), dataSchema, ERROR_MESSAGE);
    Assert.assertEquals(newDataTable.getNumberOfRows(), 0, 0);
    if (ThreadTimer.isThreadCpuTimeMeasurementEnabled()) {
      Assert.assertEquals(newDataTable.getMetadata().size(), EXPECTED_METADATA.keySet().size() + 1);
      newDataTable.getMetadata().remove(MetadataKey.RESPONSE_SER_CPU_TIME_NS.getName());
    }
    Assert.assertEquals(newDataTable.getMetadata(), EXPECTED_METADATA);
  }

  @Test
  public void testExecutionThreadCpuTimeNs()
      throws IOException {
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
    ThreadTimer.setThreadCpuTimeMeasurementEnabled(false);
    DataTable newDataTable = DataTableFactory.getDataTable(dataTable.toBytes());
    // When ThreadCpuTimeMeasurement is disabled, no value for
    // threadCpuTimeNs/systemActivitiesCpuTimeNs/responseSerializationCpuTimeNs.
    Assert.assertNull(newDataTable.getMetadata().get(MetadataKey.THREAD_CPU_TIME_NS.getName()));
    Assert.assertNull(newDataTable.getMetadata().get(MetadataKey.SYSTEM_ACTIVITIES_CPU_TIME_NS.getName()));
    Assert.assertNull(newDataTable.getMetadata().get(MetadataKey.RESPONSE_SER_CPU_TIME_NS.getName()));

    // Enable ThreadCpuTimeMeasurement, serialize/de-serialize data table.
    ThreadTimer.setThreadCpuTimeMeasurementEnabled(true);
    newDataTable = DataTableFactory.getDataTable(dataTable.toBytes());
    // When ThreadCpuTimeMeasurement is enabled, value of responseSerializationCpuTimeNs is not 0.
    Assert.assertNull(newDataTable.getMetadata().get(MetadataKey.THREAD_CPU_TIME_NS.getName()));
    Assert.assertNull(newDataTable.getMetadata().get(MetadataKey.SYSTEM_ACTIVITIES_CPU_TIME_NS.getName()));
    Assert.assertTrue(
        Integer.parseInt(newDataTable.getMetadata().get(MetadataKey.RESPONSE_SER_CPU_TIME_NS.getName())) > 0);
  }

  @Test
  public void testDataTableVer3MetadataBytesLayout()
      throws IOException {
    DataSchema.ColumnDataType[] columnDataTypes = DataSchema.ColumnDataType.values();
    int numColumns = columnDataTypes.length;
    String[] columnNames = new String[numColumns];
    for (int i = 0; i < numColumns; i++) {
      columnNames[i] = columnDataTypes[i].name();
    }

    ThreadTimer.setThreadCpuTimeMeasurementEnabled(false);
    DataSchema dataSchema = new DataSchema(columnNames, columnDataTypes);
    DataTableBuilderFactory.setDataTableVersion(DataTableFactory.VERSION_3);
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    fillDataTableWithRandomData(dataTableBuilder, columnDataTypes, numColumns);

    DataTable dataTable = dataTableBuilder.build();

    for (String key : EXPECTED_METADATA.keySet()) {
      dataTable.getMetadata().put(key, EXPECTED_METADATA.get(key));
    }

    ByteBuffer byteBuffer = ByteBuffer.wrap(dataTable.toBytes());
    int version = byteBuffer.getInt();
    Assert.assertEquals(version, DataTableFactory.VERSION_3);
    byteBuffer.getInt(); // numOfRows
    byteBuffer.getInt(); // numOfColumns
    byteBuffer.getInt(); // exceptionsStart
    byteBuffer.getInt(); // exceptionsLength
    byteBuffer.getInt(); // dictionaryMapStart
    byteBuffer.getInt(); // dictionaryMapLength
    byteBuffer.getInt(); // dataSchemaStart
    byteBuffer.getInt(); // dataSchemaLength
    byteBuffer.getInt(); // fixedSizeDataStart
    byteBuffer.getInt(); // fixedSizeDataLength
    int variableSizeDataStart = byteBuffer.getInt();
    int variableSizeDataLength = byteBuffer.getInt();

    int metadataStart = variableSizeDataStart + variableSizeDataLength;
    byteBuffer.position(metadataStart);
    int metadataLength = byteBuffer.getInt();
    byte[] metadataBytes = new byte[metadataLength];
    byteBuffer.get(metadataBytes);

    try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(metadataBytes);
        DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream)) {
      int numEntries = dataInputStream.readInt();
      // DataTable V3 and V4 serialization logic will add an extra RESPONSE_SER_CPU_TIME_NS KV pair into metadata
      Assert.assertEquals(numEntries, EXPECTED_METADATA.size());
      for (int i = 0; i < numEntries; i++) {
        int keyOrdinal = dataInputStream.readInt();
        DataTable.MetadataKey key = MetadataKey.getById(keyOrdinal);
        Assert.assertNotEquals(key, null);
        if (key.getValueType() == DataTable.MetadataValueType.INT) {
          byte[] actualBytes = new byte[Integer.BYTES];
          dataInputStream.read(actualBytes);
          Assert.assertEquals(actualBytes, Ints.toByteArray(Integer.parseInt(EXPECTED_METADATA.get(key.getName()))));
        } else if (key.getValueType() == DataTable.MetadataValueType.LONG) {
          byte[] actualBytes = new byte[Long.BYTES];
          dataInputStream.read(actualBytes);
          // Ignore the THREAD_CPU_TIME_NS/SYSTEM_ACTIVITIES_CPU_TIME_NS/RESPONSE_SER_CPU_TIME_NS key since their value
          // are evaluated during query execution.
          if (key != MetadataKey.THREAD_CPU_TIME_NS && key != MetadataKey.SYSTEM_ACTIVITIES_CPU_TIME_NS
              && key != MetadataKey.RESPONSE_SER_CPU_TIME_NS) {
            Assert.assertEquals(actualBytes, Longs.toByteArray(Long.parseLong(EXPECTED_METADATA.get(key.getName()))));
          }
        } else {
          int valueLength = dataInputStream.readInt();
          byte[] actualBytes = new byte[valueLength];
          dataInputStream.read(actualBytes);
          Assert.assertEquals(actualBytes, EXPECTED_METADATA.get(key.getName()).getBytes(UTF_8));
        }
      }
    }
  }

  private void fillDataTableWithRandomData(DataTableBuilder dataTableBuilder,
      DataSchema.ColumnDataType[] columnDataTypes, int numColumns)
      throws IOException {
    RoaringBitmap[] nullBitmaps = null;
    if (DataTableBuilderFactory.getDataTableVersion() >= DataTableFactory.VERSION_4) {
      nullBitmaps = new RoaringBitmap[numColumns];
      for (int colId = 0; colId < numColumns; colId++) {
        nullBitmaps[colId] = new RoaringBitmap();
      }
    }
    for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
      dataTableBuilder.startRow();
      for (int colId = 0; colId < numColumns; colId++) {
        // Note: isNull is handled for SV columns only for now.
        boolean isNull = nullBitmaps != null && RANDOM.nextFloat() < 0.1;
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
    RoaringBitmap[] nullBitmaps = new RoaringBitmap[numColumns];
    for (int colId = 0; colId < numColumns; colId++) {
      nullBitmaps[colId] = newDataTable.getNullRowIds(colId);
    }
    for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
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
            DataTable.CustomObject customObject = newDataTable.getCustomObject(rowId, colId);
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
          default:
            throw new UnsupportedOperationException("Unable to generate random data for: " + columnDataTypes[colId]);
        }
      }
    }
  }
}
