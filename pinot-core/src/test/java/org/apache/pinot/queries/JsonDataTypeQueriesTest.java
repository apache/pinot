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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class JsonDataTypeQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "JsonDataTypeQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final int NUM_RECORDS = 10;

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

  GenericRow createRecord(int intValue, String stringValue, String jsonValue) {
    GenericRow record = new GenericRow();
    record.putValue(INT_COLUMN, intValue);
    record.putValue(STRING_COLUMN, stringValue);
    record.putValue(JSON_COLUMN, jsonValue);

    return record;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);

    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    records.add(createRecord(1, "daffy duck",
        "{\"name\": {\"first\": \"daffy\", \"last\": \"duck\"}, \"id\": 101, \"data\": [\"a\", \"b\", \"c\", \"d\"]}"));
    records.add(createRecord(2, "mickey mouse",
        "{\"name\": {\"first\": \"mickey\", \"last\": \"mouse\"}, \"id\": 111, \"data\": [\"e\", \"b\", \"c\", \"d\"]}"));
    records.add(createRecord(3, "donald duck",
        "{\"name\": {\"first\": \"donald\", \"last\": \"duck\"}, \"id\": 121, \"data\": [\"f\", \"b\", \"c\", \"d\"]}"));
    records.add(createRecord(4, "scrooge mcduck",
        "{\"name\": {\"first\": \"scrooge\", \"last\": \"mcduck\"}, \"id\": 131, \"data\": [\"g\", \"b\", \"c\", \"d\"]}"));
    records.add(createRecord(5, "minnie mouse",
        "{\"name\": {\"first\": \"minnie\", \"last\": \"mouse\"}, \"id\": 141, \"data\": [\"h\", \"b\", \"c\", \"d\"]}"));
    records.add(createRecord(6, "daisy duck",
        "{\"name\": {\"first\": \"daisy\", \"last\": \"duck\"}, \"id\": 161.5, \"data\": [\"i\", \"b\", \"c\", \"d\"]}"));
    records.add(createRecord(7, "pluto dog",
        "{\"name\": {\"first\": \"pluto\", \"last\": \"dog\"}, \"id\": 161, \"data\": [\"j\", \"b\", \"c\", \"d\"]}"));
    records.add(createRecord(8, "goofy dwag",
        "{\"name\": {\"first\": \"goofy\", \"last\": \"dwag\"}, \"id\": 171, \"data\": [\"k\", \"b\", \"c\", \"d\"]}"));
    records.add(createRecord(9, "ludwik von drake",
        "{\"name\": {\"first\": \"ludwik\", \"last\": \"von drake\"}, \"id\": 181, \"data\": [\"l\", \"b\", \"c\", \"d\"]}"));
    records.add(createRecord(10, "nested array",
        "{\"name\":{\"first\":\"nested\",\"last\":\"array\"},\"id\":111,\"data\":[{\"e\":[{\"x\":[{\"i1\":1,\"i2\":2}]},{\"y\":[{\"i1\":1,\"i2\":2}]},{\"z\":[{\"i1\":1,\"i2\":2}]}]},{\"b\":[{\"x\":[{\"i1\":1,\"i2\":2}]},{\"y\":[{\"i1\":1,\"i2\":2}]},{\"z\":[{\"i1\":10,\"i2\":20}]}]}]}"));
    records.add(createRecord(11, "multi-dimensional-1 array",
        "{\"name\": {\"first\": \"multi-dimensional-1\",\"last\": \"array\"},\"id\": 111,\"data\": [[[1,2],[3,4]],[[\"a\",\"b\"],[\"c\",\"d\"]]]}"));
    records.add(createRecord(12, "multi-dimensional-2 array",
        "{\"name\": {\"first\": \"multi-dimensional-2\",\"last\": \"array\"},\"id\": 111,\"data\": [[[1,2],[3,4]],[[\"a\",\"b\"],[\"c\",\"d\"]]]}"));
    records.add(createRecord(13, "multi-dimensional-1 array",
        "{\"name\": {\"first\": \"multi-dimensional-1\",\"last\": \"array\"},\"id\": 111,\"data\": [[[1,2],[3,4]],[[\"a\",\"b\"],[\"c\",\"d\"]]]}"));

    List<String> jsonIndexColumns = new ArrayList<>();
    jsonIndexColumns.add("jsonColumn");
    TABLE_CONFIG.getIndexingConfig().setJsonIndexColumns(jsonIndexColumns);
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
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

  /** Verify result column type of a simple select query against JSON column */
  @Test
  public void testSimpleSelectOnJsonColumn() {
    try {
      Operator operator = getOperatorForSqlQuery("select jsonColumn FROM testTable limit 100");
      IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
      Collection<Object[]> rows = block.getSelectionResult();
      Assert.assertEquals(rows.size(), 13);
      Assert.assertEquals(block.getDataSchema().getColumnDataType(0), DataSchema.ColumnDataType.JSON);
    } catch (IllegalStateException ise) {
      Assert.assertTrue(true);
    }
  }

  /** Test filtering on string value associated with  JSON key*/
  @Test
  public void testExtractScalarWithStringFilter() {
    Operator operator = getOperatorForSqlQuery(
        "select intColumn, json_extract_scalar(jsonColumn, '$.name.last', 'STRING') FROM testTable WHERE json_extract_scalar(jsonColumn, '$.name.first', 'STRING') = 'daffy'");
    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    Collection<Object[]> rows = block.getSelectionResult();
    Assert.assertEquals(rows.size(), 1);

    Iterator<Object[]> iterator = rows.iterator();
    Assert.assertTrue(iterator.hasNext());
    Object[] row = iterator.next();
    Assert.assertEquals(row[0], 1);
    Assert.assertEquals(row[1], "duck");
  }

  /** Test filtering on number value associated with  JSON key*/
  @Test
  public void testExtractScalarWithNumericIntFilter() {
    Operator operator = getOperatorForSqlQuery(
        "select json_extract_scalar(jsonColumn, '$.name.last', 'STRING') FROM testTable WHERE json_extract_scalar(jsonColumn, '$.id', 'INT') = 171");
    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    Collection<Object[]> rows = block.getSelectionResult();
    Assert.assertEquals(rows.size(), 1);

    Iterator<Object[]> iterator = rows.iterator();
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(iterator.next()[0], "dwag");
  }

  /** Test filtering on float value associated with  JSON key*/
  @Test
  public void testExtractScalarWithNumericFloatFilter() {

    // query to retrieve result as INT
    Operator operator1 = getOperatorForSqlQuery(
        "select json_extract_scalar(jsonColumn, '$.id', 'INT') FROM testTable WHERE json_extract_scalar(jsonColumn, '$.id', 'FLOAT') = 161.5");
    IntermediateResultsBlock block1 = (IntermediateResultsBlock) operator1.nextBlock();
    Collection<Object[]> rows1 = block1.getSelectionResult();
    Assert.assertEquals(rows1.size(), 1);

    Iterator<Object[]> iterator1 = rows1.iterator();
    Assert.assertTrue(iterator1.hasNext());
    Assert.assertEquals(iterator1.next()[0], 161);

    // query to retrieve result as DOUBLE
    Operator operator2 = getOperatorForSqlQuery(
        "select json_extract_scalar(jsonColumn, '$.id', 'DOUBLE') FROM testTable WHERE json_extract_scalar(jsonColumn, '$.id', 'FLOAT') = 161.5");
    IntermediateResultsBlock block2 = (IntermediateResultsBlock) operator2.nextBlock();
    Collection<Object[]> rows2 = block2.getSelectionResult();
    Assert.assertEquals(rows2.size(), 1);

    Iterator<Object[]> iterator2 = rows2.iterator();
    Assert.assertTrue(iterator2.hasNext());
    Assert.assertEquals(iterator2.next()[0], 161.5d);
  }

  /** Retrieve JSON array after filtering on string value associated with  JSON key*/
  @Test
  public void testExtractScalarArrayWithStringFilter() {
    Operator operator = getOperatorForSqlQuery(
        "select json_extract_scalar(jsonColumn, '$.data', 'STRING') FROM testTable WHERE json_extract_scalar(jsonColumn, '$.name.first', 'STRING') = 'daffy'");
    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    Collection<Object[]> rows = block.getSelectionResult();
    Assert.assertEquals(rows.size(), 1);

    Iterator<Object[]> iterator = rows.iterator();
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(iterator.next()[0], "[\"a\",\"b\",\"c\",\"d\"]");
  }

  /** Test filtering on string value within a JSON array*/
  @Test
  public void testExtractScalarWithArrayFilter() {
    Operator operator = getOperatorForSqlQuery(
        "select json_extract_scalar(jsonColumn, '$.name.first', 'STRING') FROM testTable WHERE json_extract_scalar(jsonColumn, '$.data[0]', 'STRING') IN ('i', 'k')");
    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    Collection<Object[]> rows = block.getSelectionResult();
    Assert.assertEquals(rows.size(), 2);

    Iterator<Object[]> iterator = rows.iterator();
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(iterator.next()[0], "daisy");
    Assert.assertEquals(iterator.next()[0], "goofy");
  }

  @Test
  public void testJsonMatchWithoutIndex() {
    try {
      Operator operator = getOperatorForSqlQuery(
          "select json_extract_scalar(stringColumn, '$.name.first', 'STRING') FROM testTable WHERE json_match(stringColumn, '\"$.id\"=101')");
      Assert.assertTrue(false);
    } catch (IllegalStateException ise) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testJsonMatchAtLevel1WithIndex() {
    Operator operator = getOperatorForSqlQuery(
        "select json_extract_scalar(jsonColumn, '$.name.first', 'STRING') FROM testTable WHERE json_match(jsonColumn, '\"$.id\"=101')");

    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    Collection<Object[]> rows = block.getSelectionResult();
    Assert.assertEquals(rows.size(), 1);

    Iterator<Object[]> iterator = rows.iterator();
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(iterator.next()[0], "daffy");
  }

  @Test
  public void testJsonMatchAtLevel2WithIndex() {
    Operator operator = getOperatorForSqlQuery(
        "select json_extract_scalar(jsonColumn, '$.name.first', 'STRING') FROM testTable WHERE json_match(jsonColumn, '\"$.name.first\" = ''daffy''')");

    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    Collection<Object[]> rows = block.getSelectionResult();
    Assert.assertEquals(rows.size(), 1);

    Iterator<Object[]> iterator = rows.iterator();
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(iterator.next()[0], "daffy");
  }

  @Test
  public void testJsonMatchArrayWithIndex() {
    Operator operator = getOperatorForSqlQuery(
        "select json_extract_scalar(jsonColumn, '$.name.first', 'STRING') FROM testTable WHERE json_match(jsonColumn, '\"$.data[0]\" IN (''k'', ''j'')')");

    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    Collection<Object[]> rows = block.getSelectionResult();
    Assert.assertEquals(rows.size(), 2);

    Iterator<Object[]> iterator = rows.iterator();
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(iterator.next()[0], "pluto");
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(iterator.next()[0], "goofy");
  }

  @Test
  public void testJsonMatchNestedArrayWithIndex() {
    Operator operator = getOperatorForSqlQuery(
        "select json_extract_scalar(jsonColumn, '$.name.first', 'STRING') FROM testTable WHERE json_match(jsonColumn, '\"$.data[0].e[1].y[0].i1\" = 1')");

    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    Collection<Object[]> rows = block.getSelectionResult();
    Assert.assertEquals(rows.size(), 1);

    Iterator<Object[]> iterator = rows.iterator();
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(iterator.next()[0], "nested");
  }

  @Test
  public void testJsonMatchMultidimensionalArrayWithIndex() {
    Operator operator = getOperatorForSqlQuery(
        "select json_extract_scalar(jsonColumn, '$.name.first', 'STRING') FROM testTable WHERE json_match(jsonColumn, '\"$.data[0][1][0]\" = ''3''')");

    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    Collection<Object[]> rows = block.getSelectionResult();
    Assert.assertEquals(rows.size(), 3);

    Iterator<Object[]> iterator = rows.iterator();
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(iterator.next()[0], "multi-dimensional-1");
  }

  @Test
  public void testJsonMatchMultidimensionalArrayGroupByWithIndex() {
    Operator operator = getOperatorForSqlQuery(
        "select json_extract_scalar(jsonColumn, '$.name.first', 'STRING'), count(*) FROM testTable WHERE json_match(jsonColumn, '\"$.data[0][1][0]\" = ''3''') GROUP BY json_extract_scalar(jsonColumn, '$.name.first', 'STRING')");

    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    AggregationGroupByResult result = block.getAggregationGroupByResult();
    Iterator<GroupKeyGenerator.StringGroupKey> iterator = result.getStringGroupKeyIterator();

    Assert.assertTrue(iterator.hasNext());
    GroupKeyGenerator.StringGroupKey groupKey1 = iterator.next();
    Assert.assertEquals(groupKey1._stringKey, "multi-dimensional-2");
    Assert.assertEquals(((Long) result.getResultForKey(groupKey1, 0)).intValue(), 1l);

    Assert.assertTrue(iterator.hasNext());
    GroupKeyGenerator.StringGroupKey groupKey2 = iterator.next();
    Assert.assertEquals(groupKey2._stringKey, "multi-dimensional-1");
    Assert.assertEquals(((Long) result.getResultForKey(groupKey1, 0)).intValue(), 2l);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
