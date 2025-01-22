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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordReader;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.ComplexTypeConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.avro.Schema.*;


/**
 * Test if ComplexType (RECORD, ARRAY, MAP, UNION, ENUM, and FIXED) field from an AVRO file can be ingested into a JSON
 * column in a Pinot segment.
 */
public class JsonUnnestIngestionFromAvroQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "JsonIngestionFromAvroTest");
  private static final File AVRO_DATA_FILE = new File(INDEX_DIR, "JsonIngestionFromAvroTest.avro");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final String INT_COLUMN = "intColumn";
  private static final String JSON_COLUMN = "jsonColumn"; // for testing ARRAY of MAPS
  private static final String STRING_COLUMN = "stringColumn";
  private static final String EVENTTIME_JSON_COLUMN = "eventTimeColumn";
  //@formatter:off
  private static final org.apache.pinot.spi.data.Schema SCHEMA = new org.apache.pinot.spi.data.Schema.SchemaBuilder()
      .setSchemaName(RAW_TABLE_NAME)
      .addSingleValueDimension(INT_COLUMN, DataType.INT)
      .addSingleValueDimension(STRING_COLUMN, DataType.STRING)
      .addSingleValueDimension(JSON_COLUMN, DataType.JSON)
      .addSingleValueDimension("jsonColumn.timestamp", DataType.TIMESTAMP)
      .addSingleValueDimension("jsonColumn.data", DataType.JSON)
      .addSingleValueDimension("jsonColumn.data.a", DataType.STRING)
      .addSingleValueDimension("jsonColumn.data.b", DataType.STRING)
      .addSingleValueDimension(EVENTTIME_JSON_COLUMN, DataType.TIMESTAMP)
      .addSingleValueDimension("eventTimeColumn_10m", DataType.TIMESTAMP)
      .build();
  //@formatter:on
  private static final TableConfig TABLE_CONFIG;

  static {
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(
        List.of(new TransformConfig("eventTimeColumn", "eventTimeColumn.seconds * 1000"),
            new TransformConfig("eventTimeColumn_10m", "round(eventTimeColumn, 60000)")));
    ingestionConfig.setComplexTypeConfig(new ComplexTypeConfig(List.of(JSON_COLUMN), null, null, null));
    TABLE_CONFIG =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setIngestionConfig(ingestionConfig)
            .setJsonIndexColumns(List.of(JSON_COLUMN)).build();
  }

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
  private static GenericRow createTableRecord(int intValue, String stringValue, List<Object> arrayValue,
      Object eventTimeValue) {
    GenericRow record = new GenericRow();
    record.putValue(INT_COLUMN, intValue);
    record.putValue(STRING_COLUMN, stringValue);
    record.putValue(JSON_COLUMN, arrayValue);
    record.putValue(EVENTTIME_JSON_COLUMN, eventTimeValue);
    return record;
  }

  private static Schema createJsonRecordSchema() {
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("timestamp", create(Type.LONG)));
    fields.add(new Field("data", createMap(create(Type.STRING))));
    return createRecord("record", "doc", JsonUnnestIngestionFromAvroQueriesTest.class.getCanonicalName() + "$Json",
        false, fields);
  }

  private static Schema createEventTimeRecordSchema() {
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("seconds", create(Type.LONG)));
    return createRecord("record", "doc", JsonUnnestIngestionFromAvroQueriesTest.class.getCanonicalName() + "$EventTime",
        false, fields);
  }

  private static void createInputFile()
      throws IOException {
    INDEX_DIR.mkdir();
    Schema avroSchema = createRecord("eventsRecord", null, null, false);
    List<Field> fields = Arrays.asList(
        new Field(INT_COLUMN, createUnion(Lists.newArrayList(create(Type.INT), create(Type.NULL))), null, null),
        new Field(STRING_COLUMN, createUnion(Lists.newArrayList(create(Type.STRING), create(Type.NULL))), null, null),
        new Field(JSON_COLUMN, createArray(createJsonRecordSchema())),
        new Field(EVENTTIME_JSON_COLUMN, createEventTimeRecordSchema())

    );
    avroSchema.setFields(fields);
    List<GenericRow> inputRecords = new ArrayList<>();
    // Insert ARRAY
    inputRecords.add(
        createTableRecord(1, "daffy duck", Arrays.asList(
                new GenericRecordBuilder(createJsonRecordSchema())
                    .set("timestamp", 1719390721)
                    .set("data", Map.of("a", "1", "b", "2"))
                    .build(),
                new GenericRecordBuilder(createJsonRecordSchema())
                    .set("timestamp", 1719390722)
                    .set("data", Map.of("a", "2", "b", "4"))
                    .build()
            ),
            new GenericRecordBuilder(createEventTimeRecordSchema())
                .set("seconds", 1719390721)
                .build()));

    // Insert MAP
    inputRecords.add(
        createTableRecord(2, "mickey mouse", Arrays.asList(
                new GenericRecordBuilder(createJsonRecordSchema())
                    .set("timestamp", 1719390722)
                    .set("data", Map.of("a", "2", "b", "4"))
                    .build(),
                new GenericRecordBuilder(createJsonRecordSchema())
                    .set("timestamp", 1719390723)
                    .set("data", Map.of("a", "3", "b", "6"))
                    .build()
            ),
            new GenericRecordBuilder(createEventTimeRecordSchema())
                .set("seconds", 1719390722)
                .build()));

    inputRecords.add(
        createTableRecord(3, "donald duck", Arrays.asList(
                new GenericRecordBuilder(createJsonRecordSchema())
                    .set("timestamp", 1719390723)
                    .set("data", Map.of("a", "3", "b", "6"))
                    .build(),
                new GenericRecordBuilder(createJsonRecordSchema())
                    .set("timestamp", 1719390724)
                    .set("data", Map.of("a", "4", "b", "8"))
                    .build()
            ),
            new GenericRecordBuilder(createEventTimeRecordSchema())
                .set("seconds", 1719390723)
                .build()));

    inputRecords.add(
        createTableRecord(4, "scrooge mcduck", Arrays.asList(
                new GenericRecordBuilder(createJsonRecordSchema())
                    .set("timestamp", 1719390724)
                    .set("data", Map.of("a", "4", "b", "8"))
                    .build(),
                new GenericRecordBuilder(createJsonRecordSchema())
                    .set("timestamp", 1719390725)
                    .set("data", Map.of("a", "5", "b", "10"))
                    .build()
            ),
            new GenericRecordBuilder(createEventTimeRecordSchema())
                .set("seconds", 1719390724)
                .build()));

    // insert RECORD
    inputRecords.add(createTableRecord(5, "minney mouse", Arrays.asList(
            new GenericRecordBuilder(createJsonRecordSchema())
                .set("timestamp", 1719390725)
                .set("data", Map.of("a", "5", "b", "10"))
                .build(),
            new GenericRecordBuilder(createJsonRecordSchema())
                .set("timestamp", 1719390726)
                .set("data", Map.of("a", "6", "b", "12"))
                .build()
        ),
        new GenericRecordBuilder(createEventTimeRecordSchema())
            .set("seconds", 1719390725)
            .build()));

    // Insert simple Java String (gets converted into JSON value)
    inputRecords.add(
        createTableRecord(6, "pluto", Arrays.asList(
                new GenericRecordBuilder(createJsonRecordSchema())
                    .set("timestamp", 1719390726)
                    .set("data", Map.of("a", "6", "b", "12"))
                    .build(),
                new GenericRecordBuilder(createJsonRecordSchema())
                    .set("timestamp", 1719390727)
                    .set("data", Map.of("a", "7", "b", "14"))
                    .build()
            ),
            new GenericRecordBuilder(createEventTimeRecordSchema())
                .set("seconds", 1719390726)
                .build()));

    // Insert JSON string (gets converted into JSON document)
    inputRecords.add(
        createTableRecord(7, "scooby doo", Arrays.asList(
                new GenericRecordBuilder(createJsonRecordSchema())
                    .set("timestamp", 1719390727)
                    .set("data", Map.of("a", "7", "b", "14"))
                    .build(),
                new GenericRecordBuilder(createJsonRecordSchema())
                    .set("timestamp", 1719390728)
                    .set("data", Map.of("a", "8", "b", "16"))
                    .build()
            ),
            new GenericRecordBuilder(createEventTimeRecordSchema())
                .set("seconds", 1719390727)
                .build()));

    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, AVRO_DATA_FILE);
      for (GenericRow inputRecord : inputRecords) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(INT_COLUMN, inputRecord.getValue(INT_COLUMN));
        record.put(STRING_COLUMN, inputRecord.getValue(STRING_COLUMN));
        record.put(JSON_COLUMN, inputRecord.getValue(JSON_COLUMN));
        record.put(EVENTTIME_JSON_COLUMN, inputRecord.getValue(EVENTTIME_JSON_COLUMN));
        fileWriter.append(record);
      }
    }
  }

  private static RecordReader createRecordReader()
      throws IOException {
    Set<String> set = new HashSet<>();
    set.add(INT_COLUMN);
    set.add(STRING_COLUMN);
    set.add(JSON_COLUMN);
    set.add(EVENTTIME_JSON_COLUMN);
    AvroRecordReader avroRecordReader = new AvroRecordReader();
    avroRecordReader.init(AVRO_DATA_FILE, set, null);
    return avroRecordReader;
  }

  /** Create an AVRO file and then ingest it into Pinot while creating a JsonIndex. */
  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    createInputFile();

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setInputFilePath(AVRO_DATA_FILE.getPath());
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, createRecordReader());
    driver.build();

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(TABLE_CONFIG, SCHEMA);
    ImmutableSegment segment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), indexLoadingConfig, null);
    _indexSegment = segment;
    _indexSegments = List.of(segment, segment);
  }

  @Test
  public void testComplexSelectOnJsonColumn() {
    Operator<SelectionResultsBlock> operator = getOperator(
        "select intColumn, stringColumn, jsonColumn, \"jsonColumn.timestamp\", jsonColumn.data, jsonColumn.data.a, "
            + "jsonColumn.data.b, eventTimeColumn, eventTimeColumn_10m FROM testTable LIMIT 1000");
    SelectionResultsBlock block = operator.nextBlock();
    Collection<Object[]> rows = block.getRows();
    Assert.assertEquals(block.getDataSchema().getColumnDataType(0), DataSchema.ColumnDataType.INT);
    Assert.assertEquals(block.getDataSchema().getColumnDataType(1), DataSchema.ColumnDataType.STRING);
    Assert.assertEquals(block.getDataSchema().getColumnDataType(2), DataSchema.ColumnDataType.JSON);
    Assert.assertEquals(block.getDataSchema().getColumnDataType(3), DataSchema.ColumnDataType.TIMESTAMP);
    Assert.assertEquals(block.getDataSchema().getColumnDataType(4), DataSchema.ColumnDataType.JSON);
    Assert.assertEquals(block.getDataSchema().getColumnDataType(5), DataSchema.ColumnDataType.STRING);
    Assert.assertEquals(block.getDataSchema().getColumnDataType(6), DataSchema.ColumnDataType.STRING);
    Assert.assertEquals(block.getDataSchema().getColumnDataType(7), DataSchema.ColumnDataType.TIMESTAMP);
    Assert.assertEquals(block.getDataSchema().getColumnDataType(8), DataSchema.ColumnDataType.TIMESTAMP);

    List<String> expecteds = Arrays.asList(
        "[1, daffy duck, [{\"data\":{\"a\":\"1\",\"b\":\"2\"},\"timestamp\":1719390721},{\"data\":{\"a\":\"2\","
            + "\"b\":\"4\"},\"timestamp\":1719390722}], 1719390721, {\"a\":\"1\",\"b\":\"2\"}, 1, 2, 1719390721000, "
            + "1719390720000]",
        "[1, daffy duck, [{\"data\":{\"a\":\"1\",\"b\":\"2\"},\"timestamp\":1719390721},{\"data\":{\"a\":\"2\","
            + "\"b\":\"4\"},\"timestamp\":1719390722}], 1719390722, {\"a\":\"2\",\"b\":\"4\"}, 2, 4, 1719390721000, "
            + "1719390720000]",
        "[2, mickey mouse, [{\"data\":{\"a\":\"2\",\"b\":\"4\"},\"timestamp\":1719390722},{\"data\":{\"a\":\"3\","
            + "\"b\":\"6\"},\"timestamp\":1719390723}], 1719390722, {\"a\":\"2\",\"b\":\"4\"}, 2, 4, 1719390722000, "
            + "1719390720000]",
        "[2, mickey mouse, [{\"data\":{\"a\":\"2\",\"b\":\"4\"},\"timestamp\":1719390722},{\"data\":{\"a\":\"3\","
            + "\"b\":\"6\"},\"timestamp\":1719390723}], 1719390723, {\"a\":\"3\",\"b\":\"6\"}, 3, 6, 1719390722000, "
            + "1719390720000]",
        "[3, donald duck, [{\"data\":{\"a\":\"3\",\"b\":\"6\"},\"timestamp\":1719390723},{\"data\":{\"a\":\"4\","
            + "\"b\":\"8\"},\"timestamp\":1719390724}], 1719390723, {\"a\":\"3\",\"b\":\"6\"}, 3, 6, 1719390723000, "
            + "1719390720000]",
        "[3, donald duck, [{\"data\":{\"a\":\"3\",\"b\":\"6\"},\"timestamp\":1719390723},{\"data\":{\"a\":\"4\","
            + "\"b\":\"8\"},\"timestamp\":1719390724}], 1719390724, {\"a\":\"4\",\"b\":\"8\"}, 4, 8, 1719390723000, "
            + "1719390720000]",
        "[4, scrooge mcduck, [{\"data\":{\"a\":\"4\",\"b\":\"8\"},\"timestamp\":1719390724},{\"data\":{\"a\":\"5\","
            + "\"b\":\"10\"},\"timestamp\":1719390725}], 1719390724, {\"a\":\"4\",\"b\":\"8\"}, 4, 8, 1719390724000, "
            + "1719390720000]",
        "[4, scrooge mcduck, [{\"data\":{\"a\":\"4\",\"b\":\"8\"},\"timestamp\":1719390724},{\"data\":{\"a\":\"5\","
            + "\"b\":\"10\"},\"timestamp\":1719390725}], 1719390725, {\"a\":\"5\",\"b\":\"10\"}, 5, 10, "
            + "1719390724000, 1719390720000]",
        "[5, minney mouse, [{\"data\":{\"a\":\"5\",\"b\":\"10\"},\"timestamp\":1719390725},{\"data\":{\"a\":\"6\","
            + "\"b\":\"12\"},\"timestamp\":1719390726}], 1719390725, {\"a\":\"5\",\"b\":\"10\"}, 5, 10, "
            + "1719390725000, 1719390720000]",
        "[5, minney mouse, [{\"data\":{\"a\":\"5\",\"b\":\"10\"},\"timestamp\":1719390725},{\"data\":{\"a\":\"6\","
            + "\"b\":\"12\"},\"timestamp\":1719390726}], 1719390726, {\"a\":\"6\",\"b\":\"12\"}, 6, 12, "
            + "1719390725000, 1719390720000]",
        "[6, pluto, [{\"data\":{\"a\":\"6\",\"b\":\"12\"},\"timestamp\":1719390726},{\"data\":{\"a\":\"7\","
            + "\"b\":\"14\"},\"timestamp\":1719390727}], 1719390726, {\"a\":\"6\",\"b\":\"12\"}, 6, 12, "
            + "1719390726000, 1719390720000]",
        "[6, pluto, [{\"data\":{\"a\":\"6\",\"b\":\"12\"},\"timestamp\":1719390726},{\"data\":{\"a\":\"7\","
            + "\"b\":\"14\"},\"timestamp\":1719390727}], 1719390727, {\"a\":\"7\",\"b\":\"14\"}, 7, 14, "
            + "1719390726000, 1719390720000]",
        "[7, scooby doo, [{\"data\":{\"a\":\"7\",\"b\":\"14\"},\"timestamp\":1719390727},{\"data\":{\"a\":\"8\","
            + "\"b\":\"16\"},\"timestamp\":1719390728}], 1719390727, {\"a\":\"7\",\"b\":\"14\"}, 7, 14, "
            + "1719390727000, 1719390720000]",
        "[7, scooby doo, [{\"data\":{\"a\":\"7\",\"b\":\"14\"},\"timestamp\":1719390727},{\"data\":{\"a\":\"8\","
            + "\"b\":\"16\"},\"timestamp\":1719390728}], 1719390728, {\"a\":\"8\",\"b\":\"16\"}, 8, 16, "
            + "1719390727000, 1719390720000]");
    Assert.assertEquals(rows.size(), 14);
    int index = 0;
    for (Object[] row : rows) {
      Assert.assertEquals(Arrays.toString(row), expecteds.get(index++));
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
