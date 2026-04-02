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
package org.apache.pinot.plugin.inputformat.arrow;

import com.google.common.collect.Sets;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.AbstractRecordReaderTest;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.Test;


public class ArrowRecordReaderTest extends AbstractRecordReaderTest {
  private static final int ROWS_PER_BATCH = 1000;

  @Override
  protected RecordReader createRecordReader(File file)
      throws Exception {
    ArrowRecordReader recordReader = new ArrowRecordReader();
    recordReader.init(file, _sourceFields, null);
    return recordReader;
  }

  @Override
  protected void writeRecordsToFile(List<Map<String, Object>> recordsToWrite)
      throws Exception {
    // Single-value fields
    Field dimSvInt = new Field("dim_sv_int", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field dimSvLong = new Field("dim_sv_long", FieldType.nullable(new ArrowType.Int(64, true)), null);
    Field dimSvFloat =
        new Field("dim_sv_float", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);
    Field dimSvDouble =
        new Field("dim_sv_double", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            null);
    Field dimSvString = new Field("dim_sv_string", FieldType.nullable(new ArrowType.Utf8()), null);

    // Multi-value fields (List vectors)
    Field dimMvInt = new Field("dim_mv_int", FieldType.nullable(new ArrowType.List()),
        Arrays.asList(new Field("$data$", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    Field dimMvLong = new Field("dim_mv_long", FieldType.nullable(new ArrowType.List()),
        Arrays.asList(new Field("$data$", FieldType.nullable(new ArrowType.Int(64, true)), null)));
    Field dimMvFloat = new Field("dim_mv_float", FieldType.nullable(new ArrowType.List()),
        Arrays.asList(
            new Field("$data$", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null)));
    Field dimMvDouble = new Field("dim_mv_double", FieldType.nullable(new ArrowType.List()),
        Arrays.asList(
            new Field("$data$", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)));
    Field dimMvString = new Field("dim_mv_string", FieldType.nullable(new ArrowType.List()),
        Arrays.asList(new Field("$data$", FieldType.nullable(new ArrowType.Utf8()), null)));

    // Metric fields
    Field metInt = new Field("met_int", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field metLong = new Field("met_long", FieldType.nullable(new ArrowType.Int(64, true)), null);
    Field metFloat =
        new Field("met_float", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);
    Field metDouble =
        new Field("met_double", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);

    Schema schema = new Schema(
        Arrays.asList(dimSvInt, dimSvLong, dimSvFloat, dimSvDouble, dimSvString, dimMvInt, dimMvLong, dimMvFloat,
            dimMvDouble, dimMvString, metInt, metLong, metFloat, metDouble));

    try (RootAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        FileOutputStream fos = new FileOutputStream(_dataFile);
        FileChannel channel = fos.getChannel();
        ArrowFileWriter writer = new ArrowFileWriter(root, null, channel)) {

      writer.start();

      IntVector dimSvIntVec = (IntVector) root.getVector("dim_sv_int");
      BigIntVector dimSvLongVec = (BigIntVector) root.getVector("dim_sv_long");
      Float4Vector dimSvFloatVec = (Float4Vector) root.getVector("dim_sv_float");
      Float8Vector dimSvDoubleVec = (Float8Vector) root.getVector("dim_sv_double");
      VarCharVector dimSvStringVec = (VarCharVector) root.getVector("dim_sv_string");
      ListVector dimMvIntVec = (ListVector) root.getVector("dim_mv_int");
      ListVector dimMvLongVec = (ListVector) root.getVector("dim_mv_long");
      ListVector dimMvFloatVec = (ListVector) root.getVector("dim_mv_float");
      ListVector dimMvDoubleVec = (ListVector) root.getVector("dim_mv_double");
      ListVector dimMvStringVec = (ListVector) root.getVector("dim_mv_string");
      IntVector metIntVec = (IntVector) root.getVector("met_int");
      BigIntVector metLongVec = (BigIntVector) root.getVector("met_long");
      Float4Vector metFloatVec = (Float4Vector) root.getVector("met_float");
      Float8Vector metDoubleVec = (Float8Vector) root.getVector("met_double");

      for (int batchStart = 0; batchStart < recordsToWrite.size(); batchStart += ROWS_PER_BATCH) {
        int batchEnd = Math.min(batchStart + ROWS_PER_BATCH, recordsToWrite.size());
        int batchSize = batchEnd - batchStart;

        root.allocateNew();

        int mvIntIdx = 0;
        int mvLongIdx = 0;
        int mvFloatIdx = 0;
        int mvDoubleIdx = 0;
        int mvStringIdx = 0;

        for (int i = 0; i < batchSize; i++) {
          Map<String, Object> record = recordsToWrite.get(batchStart + i);

          dimSvIntVec.setSafe(i, (int) record.get("dim_sv_int"));
          dimSvLongVec.setSafe(i, (long) record.get("dim_sv_long"));
          dimSvFloatVec.setSafe(i, (float) record.get("dim_sv_float"));
          dimSvDoubleVec.setSafe(i, (double) record.get("dim_sv_double"));
          dimSvStringVec.setSafe(i, ((String) record.get("dim_sv_string")).getBytes());

          // Multi-value int
          List<?> mvInts = (List<?>) record.get("dim_mv_int");
          dimMvIntVec.startNewValue(i);
          IntVector mvIntChild = (IntVector) dimMvIntVec.getDataVector();
          for (Object v : mvInts) {
            mvIntChild.setSafe(mvIntIdx++, (int) v);
          }
          dimMvIntVec.endValue(i, mvInts.size());

          // Multi-value long
          List<?> mvLongs = (List<?>) record.get("dim_mv_long");
          dimMvLongVec.startNewValue(i);
          BigIntVector mvLongChild = (BigIntVector) dimMvLongVec.getDataVector();
          for (Object v : mvLongs) {
            mvLongChild.setSafe(mvLongIdx++, (long) v);
          }
          dimMvLongVec.endValue(i, mvLongs.size());

          // Multi-value float
          List<?> mvFloats = (List<?>) record.get("dim_mv_float");
          dimMvFloatVec.startNewValue(i);
          Float4Vector mvFloatChild = (Float4Vector) dimMvFloatVec.getDataVector();
          for (Object v : mvFloats) {
            mvFloatChild.setSafe(mvFloatIdx++, (float) v);
          }
          dimMvFloatVec.endValue(i, mvFloats.size());

          // Multi-value double
          List<?> mvDoubles = (List<?>) record.get("dim_mv_double");
          dimMvDoubleVec.startNewValue(i);
          Float8Vector mvDoubleChild = (Float8Vector) dimMvDoubleVec.getDataVector();
          for (Object v : mvDoubles) {
            mvDoubleChild.setSafe(mvDoubleIdx++, (double) v);
          }
          dimMvDoubleVec.endValue(i, mvDoubles.size());

          // Multi-value string
          List<?> mvStrings = (List<?>) record.get("dim_mv_string");
          dimMvStringVec.startNewValue(i);
          VarCharVector mvStringChild = (VarCharVector) dimMvStringVec.getDataVector();
          for (Object v : mvStrings) {
            mvStringChild.setSafe(mvStringIdx++, ((String) v).getBytes());
          }
          dimMvStringVec.endValue(i, mvStrings.size());

          metIntVec.setSafe(i, (int) record.get("met_int"));
          metLongVec.setSafe(i, (long) record.get("met_long"));
          metFloatVec.setSafe(i, (float) record.get("met_float"));
          metDoubleVec.setSafe(i, (double) record.get("met_double"));
        }

        dimSvIntVec.setValueCount(batchSize);
        dimSvLongVec.setValueCount(batchSize);
        dimSvFloatVec.setValueCount(batchSize);
        dimSvDoubleVec.setValueCount(batchSize);
        dimSvStringVec.setValueCount(batchSize);
        dimMvIntVec.setValueCount(batchSize);
        ((IntVector) dimMvIntVec.getDataVector()).setValueCount(mvIntIdx);
        dimMvLongVec.setValueCount(batchSize);
        ((BigIntVector) dimMvLongVec.getDataVector()).setValueCount(mvLongIdx);
        dimMvFloatVec.setValueCount(batchSize);
        ((Float4Vector) dimMvFloatVec.getDataVector()).setValueCount(mvFloatIdx);
        dimMvDoubleVec.setValueCount(batchSize);
        ((Float8Vector) dimMvDoubleVec.getDataVector()).setValueCount(mvDoubleIdx);
        dimMvStringVec.setValueCount(batchSize);
        ((VarCharVector) dimMvStringVec.getDataVector()).setValueCount(mvStringIdx);
        metIntVec.setValueCount(batchSize);
        metLongVec.setValueCount(batchSize);
        metFloatVec.setValueCount(batchSize);
        metDoubleVec.setValueCount(batchSize);
        root.setRowCount(batchSize);

        writer.writeBatch();
      }

      writer.end();
    }
  }

  @Override
  protected String getDataFileName() {
    return "data.arrow";
  }

  @Override
  protected void checkValue(RecordReader recordReader, List<Map<String, Object>> expectedRecordsMap,
      List<Object[]> expectedPrimaryKeys)
      throws Exception {
    for (int i = 0; i < expectedRecordsMap.size(); i++) {
      Map<String, Object> expectedRecord = expectedRecordsMap.get(i);
      GenericRow actualRecord = recordReader.next();
      for (FieldSpec fieldSpec : _pinotSchema.getAllFieldSpecs()) {
        String fieldSpecName = fieldSpec.getName();
        if (fieldSpec.isSingleValueField()) {
          assertSingleValueEquals(actualRecord.getValue(fieldSpecName), expectedRecord.get(fieldSpecName));
        } else {
          // Arrow converter returns List instead of Object[]
          List<?> actualList = (List<?>) actualRecord.getValue(fieldSpecName);
          List<?> expectedList = (List<?>) expectedRecord.get(fieldSpecName);
          Assert.assertEquals(actualList.size(), expectedList.size());
          for (int j = 0; j < actualList.size(); j++) {
            assertSingleValueEquals(actualList.get(j), expectedList.get(j));
          }
        }
      }
      PrimaryKey primaryKey = actualRecord.getPrimaryKey(getPrimaryKeyColumns());
      Assert.assertEquals(primaryKey.getValues(), expectedPrimaryKeys.get(i));
    }
    Assert.assertFalse(recordReader.hasNext());
  }

  @Test
  @Override
  public void testGzipRecordReader() {
    throw new SkipException("Arrow IPC file format requires seekable channels and does not support gzip compression");
  }

  @Test
  public void testFieldsToReadFiltering()
      throws Exception {
    Set<String> fieldsToRead = Sets.newHashSet("dim_sv_int", "dim_sv_string");
    try (ArrowRecordReader reader = new ArrowRecordReader()) {
      reader.init(_dataFile, fieldsToRead, null);

      Assert.assertTrue(reader.hasNext());
      GenericRow row = reader.next();

      // Requested fields should be present
      Assert.assertNotNull(row.getValue("dim_sv_int"));
      Assert.assertNotNull(row.getValue("dim_sv_string"));

      // Non-requested fields should be absent
      Assert.assertNull(row.getValue("dim_sv_long"));
      Assert.assertNull(row.getValue("dim_sv_float"));
      Assert.assertNull(row.getValue("dim_sv_double"));
      Assert.assertNull(row.getValue("met_int"));
      Assert.assertNull(row.getValue("dim_mv_int"));
    }
  }

  private void assertSingleValueEquals(Object actual, Object expected) {
    if (expected instanceof Float) {
      Assert.assertEquals(((Number) actual).floatValue(), (float) expected, 1e-6f);
    } else if (expected instanceof Double) {
      Assert.assertEquals(((Number) actual).doubleValue(), (double) expected, 1e-6d);
    } else if (expected instanceof String) {
      Assert.assertEquals(actual.toString(), expected);
    } else {
      Assert.assertEquals(actual, expected);
    }
  }
}
