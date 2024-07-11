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
package org.apache.pinot.core.segment.processing.genericrow;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.*;

import static org.apache.pinot.core.segment.processing.genericrow.GenericRowArrowFileWriter.NON_SORT_COLUMNS_DATA_DIR;
import static org.apache.pinot.core.segment.processing.genericrow.GenericRowArrowFileWriter.SORT_COLUMNS_DATA_DIR;


public class GenericRowArrowFileWriterTest {

  @Test
  public void testGenericRowArrowFileWriter()
      throws IOException {
    // Create a sample Pinot schema
    Schema pinotSchema = new Schema.SchemaBuilder().addSingleValueDimension("id", FieldSpec.DataType.INT)
        .addSingleValueDimension("name", FieldSpec.DataType.STRING).addMetric("score", FieldSpec.DataType.FLOAT)
        .addMultiValueDimension("tags", FieldSpec.DataType.STRING).build();

    // Set up the writer
    String outputDir = "arrow_dir";

    int maxBatchRows = 5;
    long maxBatchBytes = 1024 * 1024; // 1MB
    Set<String> sortColumns = new HashSet<>(Arrays.asList("id", "score"));
    GenericRowArrowFileWriter writer =
        new GenericRowArrowFileWriter(outputDir, pinotSchema, maxBatchRows, maxBatchBytes, sortColumns,
            GenericRowArrowFileWriter.ArrowCompressionType.NONE, null);

    // Create and write sample data
    List<GenericRow> testData = createTestData();
    for (GenericRow row : testData) {
      writer.writeData(row);
    }
    writer.close();

    // Verify the output
    String sortColFileName =
        StringUtils.join(new String[]{outputDir, SORT_COLUMNS_DATA_DIR, "0.arrow"}, File.separator);
    String nonSortColFileName =
        StringUtils.join(new String[]{outputDir, NON_SORT_COLUMNS_DATA_DIR, "0.arrow"}, File.separator);

    verifyArrowFile(sortColFileName, pinotSchema, sortColumns, testData);
    verifyArrowFile(nonSortColFileName, pinotSchema, new HashSet<>(Arrays.asList("name", "tags")), testData);

    // Clean up
    new File(outputDir).delete();
  }

  private List<GenericRow> createTestData() {
    List<GenericRow> data = new ArrayList<>();
    data.add(createGenericRow(1, "Alice", 85.5f, new String[]{"smart", "kind"}));
    data.add(createGenericRow(3, "Charlie", 92.0f, new String[]{"creative"}));
    data.add(createGenericRow(2, "Bob", 78.5f, new String[]{"funny", "outgoing"}));
    data.add(createGenericRow(5, "Eve", 88.0f, new String[]{"quiet", "studious"}));
    data.add(createGenericRow(4, "David", 95.5f, new String[]{"athletic", "friendly"}));
    return data;
  }

  private GenericRow createGenericRow(int id, String name, float score, String[] tags) {
    GenericRow row = new GenericRow();
    row.putValue("id", id);
    row.putValue("name", name);
    row.putValue("score", score);
    row.putValue("tags", tags);
    return row;
  }

  private void verifyArrowFile(String fileName, Schema pinotSchema, Set<String> columnsToVerify,
      List<GenericRow> expectedData)
      throws IOException {
    try (FileInputStream fileInputStream = new FileInputStream(fileName);
        FileChannel fileChannel = fileInputStream.getChannel();
        ArrowFileReader reader = new ArrowFileReader(new SeekableReadChannel(fileChannel),
            new RootAllocator(Long.MAX_VALUE))) {

      reader.loadNextBatch();
      VectorSchemaRoot root = reader.getVectorSchemaRoot();

      // Verify schema
      for (String columnName : columnsToVerify) {
        FieldVector vector = root.getVector(columnName);
        Assert.assertNotNull(vector, "Column " + columnName + " not found in Arrow file");
        verifyVectorType(vector, pinotSchema.getFieldSpecFor(columnName));
      }

      // Verify data
      List<GenericRow> sortedExpectedData = new ArrayList<>(expectedData);
      sortedExpectedData.sort(Comparator.comparingInt(row -> (Integer) row.getValue("id")));

      for (int i = 0; i < root.getRowCount(); i++) {
        for (String columnName : columnsToVerify) {
          FieldVector vector = root.getVector(columnName);
          Object actualValue = getVectorValue(vector, i);
          Object expectedValue = sortedExpectedData.get(i).getValue(columnName);
          Assert.assertEquals(actualValue, expectedValue, "Mismatch in column " + columnName + " at row " + i);
        }
      }
    }
  }

  private void verifyVectorType(FieldVector vector, FieldSpec fieldSpec) {
    if (fieldSpec.isSingleValueField()) {
      verifyPrimitiveDataType(vector, fieldSpec.getDataType().getStoredType());
    } else {
      Assert.assertTrue(vector instanceof ListVector);
      FieldVector dataVector = ((ListVector) vector).getDataVector();
      verifyPrimitiveDataType(dataVector, fieldSpec.getDataType().getStoredType());
    }
  }

  private static void verifyPrimitiveDataType(FieldVector vector, FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        Assert.assertTrue(vector instanceof IntVector);
        break;
      case LONG:
        Assert.assertTrue(vector instanceof BigIntVector);
        break;
      case FLOAT:
        Assert.assertTrue(vector instanceof Float4Vector);
        break;
      case DOUBLE:
        Assert.assertTrue(vector instanceof Float8Vector);
        break;
      case STRING:
        Assert.assertTrue(vector instanceof VarCharVector);
        break;
      default:
        Assert.fail("Unsupported data type: " + dataType);
    }
  }

  private Object getVectorValue(FieldVector vector, int index) {
    if (vector instanceof IntVector) {
      return ((IntVector) vector).get(index);
    } else if (vector instanceof BigIntVector) {
      return ((BigIntVector) vector).get(index);
    } else if (vector instanceof Float4Vector) {
      return ((Float4Vector) vector).get(index);
    } else if (vector instanceof Float8Vector) {
      return ((Float8Vector) vector).get(index);
    } else if (vector instanceof VarCharVector) {
      return new String(((VarCharVector) vector).get(index));
    } else if (vector instanceof ListVector) {
      return getListVectorValue((ListVector) vector, index);
    }
    throw new UnsupportedOperationException("Unsupported vector type: " + vector.getClass().getSimpleName());
  }

  private Object getListVectorValue(ListVector listVector, int index) {
    int start = listVector.getOffsetBuffer().getInt(index * ListVector.OFFSET_WIDTH);
    int end = listVector.getOffsetBuffer().getInt((index + 1) * ListVector.OFFSET_WIDTH);
    int length = end - start;

    FieldVector dataVector = listVector.getDataVector();

    if (dataVector instanceof IntVector) {
      int[] result = new int[length];
      for (int i = 0; i < length; i++) {
        result[i] = ((IntVector) dataVector).get(start + i);
      }
      return result;
    } else if (dataVector instanceof BigIntVector) {
      long[] result = new long[length];
      for (int i = 0; i < length; i++) {
        result[i] = ((BigIntVector) dataVector).get(start + i);
      }
      return result;
    } else if (dataVector instanceof Float4Vector) {
      float[] result = new float[length];
      for (int i = 0; i < length; i++) {
        result[i] = ((Float4Vector) dataVector).get(start + i);
      }
      return result;
    } else if (dataVector instanceof Float8Vector) {
      double[] result = new double[length];
      for (int i = 0; i < length; i++) {
        result[i] = ((Float8Vector) dataVector).get(start + i);
      }
      return result;
    } else if (dataVector instanceof VarCharVector) {
      String[] result = new String[length];
      for (int i = 0; i < length; i++) {
        result[i] = new String(((VarCharVector) dataVector).get(start + i));
      }
      return result;
    }

    throw new UnsupportedOperationException("Unsupported vector type: " + dataVector.getClass().getSimpleName());
  }
}
