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
package org.apache.pinot.spi.data.readers;

import com.google.common.collect.Sets;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Lists;


public abstract class AbstractRecordReaderTest {
  private final static Random RANDOM = new Random(System.currentTimeMillis());
  protected final static int SAMPLE_RECORDS_SIZE = 10000;

  protected final File _tempDir = new File(FileUtils.getTempDirectory(), getClass().getSimpleName());
  protected final File _dataFile = new File(_tempDir, getDataFileName());
  protected List<Map<String, Object>> _records;
  protected List<Object[]> _primaryKeys;
  protected org.apache.pinot.spi.data.Schema _pinotSchema;
  protected Set<String> _sourceFields;
  protected RecordReader _recordReader;

  protected static List<Map<String, Object>> generateRandomRecords(Schema pinotSchema) {
    List<Map<String, Object>> records = new ArrayList<>();

    for (int i = 0; i < SAMPLE_RECORDS_SIZE; i++) {
      Map<String, Object> recordMap = new HashMap<>();
      for (FieldSpec fieldSpec : pinotSchema.getAllFieldSpecs()) {
        recordMap.put(fieldSpec.getName(), generateRandomValue(fieldSpec));
      }
      records.add(recordMap);
    }
    return records;
  }

  protected static List<Object[]> generatePrimaryKeys(List<Map<String, Object>> records,
      List<String> primaryKeyColumns) {
    List<Object[]> primaryKeys = Lists.newArrayList();
    for (Map<String, Object> record : records) {
      Object[] primaryKey = new Object[primaryKeyColumns.size()];
      for (int i = 0; i < primaryKeyColumns.size(); i++) {
        String primaryKeyColumn = primaryKeyColumns.get(i);
        primaryKey[i] = record.get(primaryKeyColumn);
      }
      primaryKeys.add(primaryKey);
    }
    return primaryKeys;
  }

  private static Object generateRandomValue(FieldSpec fieldSpec) {
    if (fieldSpec.isSingleValueField()) {
      return generateRandomSingleValue(fieldSpec);
    }
    List list = new ArrayList();
    int listSize = 1 + RANDOM.nextInt(50);
    for (int i = 0; i < listSize; i++) {
      list.add(generateRandomSingleValue(fieldSpec));
    }
    return list;
  }

  private static Object generateRandomSingleValue(FieldSpec fieldSpec) {
    switch (fieldSpec.getDataType()) {
      case INT:
        return RANDOM.nextInt();
      case LONG:
        return RANDOM.nextLong();
      case FLOAT:
        return RANDOM.nextFloat();
      case DOUBLE:
        return RANDOM.nextDouble();
      case STRING:
        return RandomStringUtils.randomAscii(RANDOM.nextInt(50) + 1);
      case BOOLEAN:
        return RANDOM.nextBoolean();
      default:
        throw new RuntimeException("Not supported fieldSpec - " + fieldSpec);
    }
  }

  protected void checkValue(RecordReader recordReader, List<Map<String, Object>> expectedRecordsMap,
      List<Object[]> expectedPrimaryKeys)
      throws Exception {
    for (int i = 0; i < expectedRecordsMap.size(); i++) {
      Map<String, Object> expectedRecord = expectedRecordsMap.get(i);
      GenericRow actualRecord = recordReader.next();
      for (FieldSpec fieldSpec : _pinotSchema.getAllFieldSpecs()) {
        String fieldSpecName = fieldSpec.getName();
        if (fieldSpec.isSingleValueField()) {
          Assert.assertEquals(actualRecord.getValue(fieldSpecName), expectedRecord.get(fieldSpecName));
        } else {
          Object[] actualRecords = (Object[]) actualRecord.getValue(fieldSpecName);
          List expectedRecords = (List) expectedRecord.get(fieldSpecName);
          if (expectedRecords != null) {
            Assert.assertEquals(actualRecords.length, expectedRecords.size());
            for (int j = 0; j < actualRecords.length; j++) {
              Assert.assertEquals(actualRecords[j], expectedRecords.get(j));
            }
          }
        }
      }
      PrimaryKey primaryKey = actualRecord.getPrimaryKey(getPrimaryKeyColumns());
      Assert.assertEquals(primaryKey.getValues(), expectedPrimaryKeys.get(i));
    }
    Assert.assertFalse(recordReader.hasNext());
  }

  protected org.apache.pinot.spi.data.Schema getPinotSchema() {
    return new org.apache.pinot.spi.data.Schema.SchemaBuilder()
        .addSingleValueDimension("dim_sv_int", FieldSpec.DataType.INT)
        .addSingleValueDimension("dim_sv_long", FieldSpec.DataType.LONG)
        .addSingleValueDimension("dim_sv_float", FieldSpec.DataType.FLOAT)
        .addSingleValueDimension("dim_sv_double", FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension("dim_sv_string", FieldSpec.DataType.STRING)
        .addMultiValueDimension("dim_mv_int", FieldSpec.DataType.INT)
        .addMultiValueDimension("dim_mv_long", FieldSpec.DataType.LONG)
        .addMultiValueDimension("dim_mv_float", FieldSpec.DataType.FLOAT)
        .addMultiValueDimension("dim_mv_double", FieldSpec.DataType.DOUBLE)
        .addMultiValueDimension("dim_mv_string", FieldSpec.DataType.STRING).addMetric("met_int", FieldSpec.DataType.INT)
        .addMetric("met_long", FieldSpec.DataType.LONG).addMetric("met_float", FieldSpec.DataType.FLOAT)
        .addMetric("met_double", FieldSpec.DataType.DOUBLE).build();
  }

  protected List<String> getPrimaryKeyColumns() {
    return Lists.newArrayList("dim_sv_int", "dim_sv_string");
  }

  protected Set<String> getSourceFields(Schema schema) {
    Set<String> sourceFields = Sets.newHashSet(schema.getColumnNames());
    sourceFields.add("column_not_in_source");
    return sourceFields;
  }

  protected File compressFileWithGzip(String sourcePath, String targetPath)
      throws IOException {
    try (GZIPOutputStream gos = new GZIPOutputStream(new FileOutputStream(Paths.get(targetPath).toFile()))) {
      Files.copy(Paths.get(sourcePath), gos);
      return new File(targetPath);
    }
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    if (_tempDir.exists()) {
      FileUtils.cleanDirectory(_tempDir);
    }
    FileUtils.forceMkdir(_tempDir);
    // Generate Pinot schema
    _pinotSchema = getPinotSchema();
    _sourceFields = getSourceFields(_pinotSchema);
    // Generate random records based on Pinot schema
    _records = generateRandomRecords(_pinotSchema);
    _primaryKeys = generatePrimaryKeys(_records, getPrimaryKeyColumns());
    // Write generated random records to file
    writeRecordsToFile(_records);
    // Create and init RecordReader
    _recordReader = createRecordReader();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    FileUtils.forceDelete(_tempDir);
  }

  @Test
  public void testRecordReader()
      throws Exception {
    checkValue(_recordReader, _records, _primaryKeys);
    _recordReader.rewind();
    checkValue(_recordReader, _records, _primaryKeys);
  }

  @Test
  public void testGzipRecordReader()
      throws Exception {
    // Test Gzipped Avro file that ends with ".gz"
    File gzipDataFile = new File(_tempDir, _dataFile.getName() + ".gz");
    compressFileWithGzip(_dataFile.getAbsolutePath(), gzipDataFile.getAbsolutePath());
    RecordReader recordReader = createRecordReader(gzipDataFile);
    checkValue(recordReader, _records, _primaryKeys);
    recordReader.rewind();
    checkValue(recordReader, _records, _primaryKeys);

    // Test Gzipped Avro file that doesn't end with '.gz'.
    File gzipDataFile2 = new File(_tempDir, _dataFile.getName() + ".test");
    compressFileWithGzip(_dataFile.getAbsolutePath(), gzipDataFile2.getAbsolutePath());
    recordReader = createRecordReader(gzipDataFile2);
    checkValue(recordReader, _records, _primaryKeys);
    recordReader.rewind();
    checkValue(recordReader, _records, _primaryKeys);
  }

  /**
   * Create the record reader given a file
   *
   * @param file input file
   * @return an implementation of RecordReader of the given file
   * @throws Exception
   */
  protected abstract RecordReader createRecordReader(File file)
      throws Exception;

  /**
   * @return an implementation of RecordReader
   * @throws Exception
   */
  protected RecordReader createRecordReader()
      throws Exception {
    return createRecordReader(_dataFile);
  }

  /**
   * Write records into a file
   * @throws Exception
   */
  protected abstract void writeRecordsToFile(List<Map<String, Object>> recordsToWrite)
      throws Exception;

  /**
   * Get data file name
   * @throws Exception
   */
  protected abstract String getDataFileName();
}
