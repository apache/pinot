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

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class JsonResponseEncoderTest {

  @Test
  public void testEncodeDecodeSingleRow() throws IOException {
    // Define a schema with two columns: one integer and one string.
    DataSchema schema = new DataSchema(
        new String[] {"col1", "col2"},
        new ColumnDataType[] {ColumnDataType.INT, ColumnDataType.STRING});

    // Create a single row for testing.
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {123, "test value"});

    // Construct the ResultTable.
    ResultTable resultTable = new ResultTable(schema, rows);

    // Instantiate the JSON encoder.
    JsonResponseEncoder encoder = new JsonResponseEncoder();

    // Encode the entire table (starting at row 0).
    byte[] encodedBytes = encoder.encodeResultTable(resultTable, 0, rows.size());

    // Decode the table back.
    ResultTable decodedTable = encoder.decodeResultTable(encodedBytes, rows.size(), schema);

    // Verify that the schemas match.
    assertEquals(resultTable.getDataSchema().toString(),
        decodedTable.getDataSchema().toString(), "Schemas should match");

    // Verify that the row count and row contents are identical.
    assertEquals(resultTable.getRows().size(), decodedTable.getRows().size(), "Row count should match");
    for (int i = 0; i < rows.size(); i++) {
      Object[] row = rows.get(i);
      for (int j = 0; j < row.length; j++) {
        assertEquals(row[j], decodedTable.getRows().get(i)[j], "Row " + i + " should match");
      }
    }
  }

  @Test
  public void testEncodeDecodeMultipleRows() throws IOException {
    // Define a schema with three columns.
    DataSchema schema = new DataSchema(
        new String[] {"id", "name", "score"},
        new ColumnDataType[] {ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.DOUBLE});

    // Create multiple rows.
    List<Object[]> rows = Arrays.asList(
        new Object[] {1, "Alice", 95.5},
        new Object[] {2, "Bob", 88.0},
        new Object[] {3, "Charlie", 76.3}
    );

    ResultTable resultTable = new ResultTable(schema, rows);
    JsonResponseEncoder encoder = new JsonResponseEncoder();

    // Encode a sub-range: rows 1 and 2 (i.e. starting at index 1, length 2)
    byte[] encodedBytes = encoder.encodeResultTable(resultTable, 1, 2);

    // Decode the subrange.
    ResultTable decodedTable = encoder.decodeResultTable(encodedBytes, 2, schema);

    // Verify that we got two rows.
    assertEquals(2, decodedTable.getRows().size(), "Should decode two rows");

    // Verify that the decoded rows match rows 1 and 2 of the original table.
    for (int i = 0; i < 2; i++) {
      Object[] row = rows.get(i + 1);
      for (int j = 0; j < row.length; j++) {
        Object actual = decodedTable.getRows().get(i)[j];
        Object expected = row[j];
        assertEquals(actual, expected, "Row " + (i + 1) + " should match");
      }
    }
  }

  @Test
  public void testEncodeDecodeAllDataTypes() throws IOException {
    // Define the column names and corresponding data types.
    String[] columnNames = {
        "intCol", "longCol", "floatCol", "doubleCol", "bigDecimalCol", "booleanCol", "timestampCol",
        "stringCol", "jsonCol", "mapCol", "bytesCol", "objectCol", "intArrayCol", "longArrayCol",
        "floatArrayCol", "doubleArrayCol", "booleanArrayCol", "timestampArrayCol", "stringArrayCol",
        "bytesArrayCol", "unknownCol"
    };

    DataSchema.ColumnDataType[] columnTypes = {
        ColumnDataType.INT,
        ColumnDataType.LONG,
        ColumnDataType.FLOAT,
        ColumnDataType.DOUBLE,
        ColumnDataType.BIG_DECIMAL,
        ColumnDataType.BOOLEAN,
        ColumnDataType.TIMESTAMP,
        ColumnDataType.STRING,
        ColumnDataType.JSON,
        ColumnDataType.MAP,
        ColumnDataType.BYTES,
        ColumnDataType.OBJECT,
        ColumnDataType.INT_ARRAY,
        ColumnDataType.LONG_ARRAY,
        ColumnDataType.FLOAT_ARRAY,
        ColumnDataType.DOUBLE_ARRAY,
        ColumnDataType.BOOLEAN_ARRAY,
        ColumnDataType.TIMESTAMP_ARRAY,
        ColumnDataType.STRING_ARRAY,
        ColumnDataType.BYTES_ARRAY,
        ColumnDataType.UNKNOWN
    };

    DataSchema schema = new DataSchema(columnNames, columnTypes);

    // Create test values for each type.
    Integer intVal = 42;
    Long longVal = 1234567890123L;
    Float floatVal = 3.14f;
    Double doubleVal = 2.71828;
    BigDecimal bigDecimalVal = new BigDecimal("12345.6789");
    Boolean booleanVal = true;
    Timestamp timestampVal = new Timestamp(1622548800000L);
    String stringVal = "hello";
    String jsonVal = "{\"key\":\"value\"}";
    Map<String, Integer> mapVal = new HashMap<>();
    mapVal.put("a", 1);
    mapVal.put("b", 2);
    byte[] bytesVal = new byte[] {1, 2, 3, 4};
    String objectVal = "customObject";
    int[] intArrayVal = new int[] {1, 2, 3};
    long[] longArrayVal = new long[] {4L, 5L, 6L};
    float[] floatArrayVal = new float[] {7.7f, 8.8f};
    double[] doubleArrayVal = new double[] {9.9, 10.1};
    boolean[] booleanArrayVal = new boolean[] {true, false, true};
    Timestamp[] timestampArrayVal = new Timestamp[] {
        new Timestamp(1622548800000L), new Timestamp(1622548801000L)
    };
    String[] stringArrayVal = new String[] {"a", "b", "c"};
    byte[][] bytesArrayVal = new byte[][] {
        new byte[] {1, 2}, new byte[] {3, 4}
    };
    Object unknownVal = null;  // UNKNOWN is represented as null in this example.

    // Build a single row that contains all the above values.
    List<Object[]> rows = new ArrayList<>();
    Object[] row = new Object[] {
        intVal, longVal, floatVal, doubleVal, bigDecimalVal, booleanVal, timestampVal, stringVal,
        jsonVal, mapVal, bytesVal, objectVal, intArrayVal, longArrayVal, floatArrayVal, doubleArrayVal,
        booleanArrayVal, timestampArrayVal, stringArrayVal, bytesArrayVal, unknownVal
    };

    // Convert each value using the schema's formatting (if needed).
    for (int i = 0; i < row.length; i++) {
      row[i] = columnTypes[i].format(row[i]);
    }
    rows.add(row);
    ResultTable resultTable = new ResultTable(schema, rows);

    // Instantiate the JSON encoder and encode the ResultTable.
    JsonResponseEncoder encoder = new JsonResponseEncoder();
    byte[] encodedBytes = encoder.encodeResultTable(resultTable, 0, rows.size());

    // Decode the byte array back into a ResultTable.
    ResultTable decodedTable = encoder.decodeResultTable(encodedBytes, rows.size(), schema);

    // There should be exactly one row in the decoded table.
    assertEquals(1, decodedTable.getRows().size(), "Row count should be 1");
    Object[] decodedRow = decodedTable.getRows().get(0);
    assertEquals(row.length, decodedRow.length, "Column count should match");

    // Compare each column. For array types, use appropriate array comparisons.
    for (int i = 0; i < row.length; i++) {
      Object original = row[i];
      Object decoded = decodedRow[i];
      if (original != null && original.getClass().isArray()) {
        // Compare primitive arrays or Object arrays.
        if (original instanceof boolean[]) {
          assertEquals(Arrays.toString((boolean[]) original), Arrays.toString((boolean[]) decoded),
              "Column " + columnNames[i] + " should match");
        } else if (original instanceof int[]) {
          assertEquals(Arrays.toString((int[]) original), Arrays.toString((int[]) decoded),
              "Column " + columnNames[i] + " should match");
        } else if (original instanceof long[]) {
          assertEquals(Arrays.toString((long[]) original), Arrays.toString((long[]) decoded),
              "Column " + columnNames[i] + " should match");
        } else if (original instanceof float[]) {
          assertEquals(Arrays.toString((float[]) original), Arrays.toString((float[]) decoded),
              "Column " + columnNames[i] + " should match");
        } else if (original instanceof double[]) {
          assertEquals(Arrays.toString((double[]) original), Arrays.toString((double[]) decoded),
              "Column " + columnNames[i] + " should match");
        } else if (original instanceof Object[]) { // e.g., Timestamp[], String[], or byte[][]
          assertEquals(Arrays.deepToString((Object[]) original), Arrays.deepToString((Object[]) decoded),
              "Column " + columnNames[i] + " should match");
        } else if (original instanceof byte[]) {
          assertEquals(Arrays.toString((byte[]) original), Arrays.toString((byte[]) decoded),
              "Column " + columnNames[i] + " should match");
        } else {
          fail("Unsupported array type for column " + columnNames[i]);
        }
      } else {
        // For non-array types, check equality directly.
        assertEquals(original, decoded, "Column " + columnNames[i] + " should match");
      }
    }
  }
}
