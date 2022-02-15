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
package org.apache.pinot.queries;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordReader;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.Pair;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.avro.Schema.*;


/**
 * Test if ComplexType (RECORD, ARRAY, MAP, and UNION) field from an AVRO file can be ingested into a JSON column in
 * a Pinot segment. This class tests. Ingestion from ENUM (symbol) and FIXED (binary) is not supported.
 */
public class JsonIngestionFromAvroQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "JsonIngestionFromAvroTest");
  private static final File AVRO_DATA_FILE = new File(INDEX_DIR, "JsonIngestionFromAvroTest.avro");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final String INT_COLUMN = "intColumn";
  private static final String JSON_COLUMN = "jsonColumn";
  private static final String STRING_COLUMN = "stringColumn";
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN, FieldSpec.DataType.INT)
          .addSingleValueDimension(JSON_COLUMN, FieldSpec.DataType.JSON)
          .addSingleValueDimension(STRING_COLUMN, FieldSpec.DataType.STRING).build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  /** @return {@link GenericRow} representing a row in Pinot table. */
  private static GenericRow createTableRecord(int intValue, String stringValue, Object jsonValue) {
    GenericRow record = new GenericRow();
    record.putValue(INT_COLUMN, intValue);
    record.putValue(STRING_COLUMN, stringValue);
    record.putValue(JSON_COLUMN, jsonValue);

    return record;
  }

  private static Map<String, String> createMapField(Pair<String, String>[] pairs) {
    Map<String, String> map = new HashMap<>();
    for (Pair<String, String> pair : pairs) {
      map.put(pair.getFirst(), pair.getSecond());
    }
    return map;
  }

  private static org.apache.avro.Schema createRecordSchema() {
    List<org.apache.avro.Schema.Field> fields = new ArrayList<>();
    fields.add(new org.apache.avro.Schema.Field("id", create(Type.INT)));
    fields.add(new org.apache.avro.Schema.Field("name", create(Type.STRING)));
    return createRecord("record", "doc", JsonIngestionFromAvroQueriesTest.class.getCanonicalName(), false, fields);
  }

  public static GenericData.Record createRecordField(String k1, int v1, String k2, String v2) {
    GenericData.Record record = new GenericData.Record(createRecordSchema());
    record.put(k1, v1);
    record.put(k2, v2);
    return record;
  }

  protected void createInputFile()
      throws IOException {
    INDEX_DIR.mkdir();
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("eventsRecord", null, null, false);
    List<Field> fields = Arrays
        .asList(new Field(INT_COLUMN, createUnion(Lists.newArrayList(create(Type.INT), create(Type.NULL))), null, null),
            new Field(STRING_COLUMN, createUnion(Lists.newArrayList(create(Type.STRING), create(Type.NULL))), null,
                null), new Field(JSON_COLUMN,
                createUnion(createArray(create(Type.STRING)), createMap(create(Type.STRING)), createRecordSchema(),
                    create(Type.NULL))));
    avroSchema.setFields(fields);
    List<GenericRow> inputRecords = new ArrayList<>();
    // insert ARRAY
    inputRecords.add(createTableRecord(1, "daffy duck", Arrays.asList("this", "is", "a", "test")));

    // insert MAP
    inputRecords
        .add(createTableRecord(2, "mickey mouse", createMapField(new Pair[]{new Pair("a", "1"), new Pair("b", "2")})));
    inputRecords
        .add(createTableRecord(3, "donald duck", createMapField(new Pair[]{new Pair("a", "1"), new Pair("b", "2")})));
    inputRecords.add(
        createTableRecord(4, "scrooge mcduck", createMapField(new Pair[]{new Pair("a", "1"), new Pair("b", "2")})));

    // insert RECORD
    inputRecords.add(createTableRecord(5, "minney mouse", createRecordField("id", 1, "name", "minney")));

    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, AVRO_DATA_FILE);
      for (GenericRow inputRecord : inputRecords) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(INT_COLUMN, inputRecord.getValue(INT_COLUMN));
        record.put(STRING_COLUMN, inputRecord.getValue(STRING_COLUMN));
        record.put(JSON_COLUMN, inputRecord.getValue(JSON_COLUMN));
        fileWriter.append(record);
      }
    }
  }

  protected RecordReader createRecordReader(Set<String> fieldsToRead)
      throws IOException {
    AvroRecordReader avroRecordReader = new AvroRecordReader();
    avroRecordReader.init(AVRO_DATA_FILE, fieldsToRead, null);
    return avroRecordReader;
  }

  /** Create an AVRO file and then ingest it into Pinot while creating a JsonIndex. */
  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    createInputFile();

    List<String> jsonIndexColumns = new ArrayList<>();
    jsonIndexColumns.add("jsonColumn");
    TABLE_CONFIG.getIndexingConfig().setJsonIndexColumns(jsonIndexColumns);
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());
    segmentGeneratorConfig.setInputFilePath(AVRO_DATA_FILE.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, createRecordReader(Set.of(INT_COLUMN, STRING_COLUMN, JSON_COLUMN)));
    driver.build();

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setTableConfig(TABLE_CONFIG);
    indexLoadingConfig.setJsonIndexColumns(new HashSet<String>(jsonIndexColumns));
    indexLoadingConfig.setReadMode(ReadMode.mmap);

    ImmutableSegment immutableSegment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), indexLoadingConfig);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  /** Verify that we can query the JSON column that ingested ComplexType data from an AVRO file (see setUp). */
  @Test
  public void testSimpleSelectOnJsonColumn() {
    try {
      Operator operator = getOperatorForSqlQuery("select intColumn, stringColumn, jsonColumn FROM testTable limit 100");
      IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
      Collection<Object[]> rows = block.getSelectionResult();
      Assert.assertEquals(block.getDataSchema().getColumnDataType(0), DataSchema.ColumnDataType.INT);
      Assert.assertEquals(block.getDataSchema().getColumnDataType(1), DataSchema.ColumnDataType.STRING);
      Assert.assertEquals(block.getDataSchema().getColumnDataType(2), DataSchema.ColumnDataType.JSON);

      List<String> expecteds = Arrays
          .asList("[1, daffy duck, [\"this\",\"is\",\"a\",\"test\"]]", "[2, mickey mouse, {\"a\":\"1\",\"b\":\"2\"}]",
              "[3, donald duck, {\"a\":\"1\",\"b\":\"2\"}]", "[4, scrooge mcduck, {\"a\":\"1\",\"b\":\"2\"}]",
              "[5, minney mouse, {\"name\":\"minney\",\"id\":1}]");
      int index = 0;

      Iterator<Object[]> iterator = rows.iterator();
      while (iterator.hasNext()) {
        Object[] row = iterator.next();
        System.out.println(Arrays.toString(row));
        Assert.assertEquals(Arrays.toString(row), expecteds.get(index++));
      }
    } catch (IllegalStateException ise) {
      Assert.assertTrue(true);
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
