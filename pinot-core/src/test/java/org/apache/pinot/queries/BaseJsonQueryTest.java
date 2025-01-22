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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;


public abstract class BaseJsonQueryTest extends BaseQueriesTest {

  static final String RAW_TABLE_NAME = "testTable";
  static final String SEGMENT_NAME = "testSegment";

  static final String INT_COLUMN = "intColumn";
  static final String LONG_COLUMN = "longColumn";
  static final String STRING_COLUMN = "stringColumn";
  static final String JSON_COLUMN = "jsonColumn";
  static final String RAW_JSON_COLUMN = "rawJsonColumn";
  static final String RAW_BYTES_COLUMN = "rawBytesColumn";
  static final String DICTIONARY_BYTES_COLUMN = "dictionaryBytesColumn";
  static final String RAW_STRING_COLUMN = "rawStringColumn";
  static final String DICTIONARY_STRING_COLUMN = "dictionaryStringColumn";
  static final String JSON_COLUMN_WITHOUT_INDEX = "jsonColumnWithoutIndex";

  @DataProvider
  public static Object[][] allJsonColumns() {
    return new Object[][]{
        {JSON_COLUMN},
        {RAW_JSON_COLUMN},
        {JSON_COLUMN_WITHOUT_INDEX},
        {RAW_BYTES_COLUMN},
        {DICTIONARY_BYTES_COLUMN},
        {RAW_STRING_COLUMN},
        {DICTIONARY_STRING_COLUMN},
    };
  }

  @DataProvider
  public static Object[][] nativeJsonColumns() {
    return new Object[][]{
        {JSON_COLUMN},
        {RAW_JSON_COLUMN},
        {JSON_COLUMN_WITHOUT_INDEX},
    };
  }

  @DataProvider
  public static Object[][] nonNativeJsonColumns() {
    // columns where we should be able to extract JSON with a function, but can't use all the literal features
    return new Object[][]{
        {RAW_BYTES_COLUMN},
        {DICTIONARY_BYTES_COLUMN},
        {RAW_STRING_COLUMN},
        {DICTIONARY_STRING_COLUMN},
    };
  }

  protected IndexSegment _indexSegment;
  protected List<IndexSegment> _indexSegments;

  @BeforeClass
  public void setUp()
      throws Exception {
    File indexDir = indexDir();
    FileUtils.deleteDirectory(indexDir);

    TableConfig tableConfig = tableConfig();
    Schema schema = schema();

    List<GenericRow> records = new ArrayList<>(numRecords());
    records.add(createRecord(1, 1, "daffy duck",
        "{\"name\": {\"first\": \"daffy\", \"last\": \"duck\"}, \"id\": 101, \"data\": [\"a\", \"b\", \"c\", \"d\"]}"));
    records.add(createRecord(2, 2, "mickey mouse",
        "{\"name\": {\"first\": \"mickey\", \"last\": \"mouse\"}, \"id\": 111, \"data\": [\"e\", \"b\", \"c\", "
            + "\"d\"]}"));
    records.add(createRecord(3, 3, "donald duck",
        "{\"name\": {\"first\": \"donald\", \"last\": \"duck\"}, \"id\": 121, \"data\": [\"f\", \"b\", \"c\", "
            + "\"d\"]}"));
    records.add(createRecord(4, 4, "scrooge mcduck",
        "{\"name\": {\"first\": \"scrooge\", \"last\": \"mcduck\"}, \"id\": 131, \"data\": [\"g\", \"b\", \"c\", "
            + "\"d\"]}"));
    records.add(createRecord(5, 5, "minnie mouse",
        "{\"name\": {\"first\": \"minnie\", \"last\": \"mouse\"}, \"id\": 141, \"data\": [\"h\", \"b\", \"c\", "
            + "\"d\"]}"));
    records.add(createRecord(6, 6, "daisy duck",
        "{\"name\": {\"first\": \"daisy\", \"last\": \"duck\"}, \"id\": 161.5, \"data\": [\"i\", \"b\", \"c\", "
            + "\"d\"]}"));
    records.add(createRecord(7, 7, "pluto dog",
        "{\"name\": {\"first\": \"pluto\", \"last\": \"dog\"}, \"id\": 161, \"data\": [\"j\", \"b\", \"c\", \"d\"]}"));
    records.add(createRecord(8, 8, "goofy dwag",
        "{\"name\": {\"first\": \"goofy\", \"last\": \"dwag\"}, \"id\": 171, \"data\": [\"k\", \"b\", \"c\", \"d\"]}"));
    records.add(createRecord(9, 9, "ludwik von drake",
        "{\"name\": {\"first\": \"ludwik\", \"last\": \"von drake\"}, \"id\": 181, \"data\": [\"l\", \"b\", \"c\", "
            + "\"d\"]}"));
    records.add(createRecord(10, 10, "nested array",
        "{\"name\":{\"first\":\"nested\",\"last\":\"array\"},\"id\":111,\"data\":[{\"e\":[{\"x\":[{\"i1\":1,"
            + "\"i2\":2}]},{\"y\":[{\"i1\":1,\"i2\":2}]},{\"z\":[{\"i1\":1,\"i2\":2}]}]},{\"b\":[{\"x\":[{\"i1\":1,"
            + "\"i2\":2}]},{\"y\":[{\"i1\":1,\"i2\":2}]},{\"z\":[{\"i1\":10,\"i2\":20}]}]}]}"));
    records.add(createRecord(11, 11, "multi-dimensional-1 array",
        "{\"name\": {\"first\": \"multi-dimensional-1\",\"last\": \"array\"},\"id\": 111,\"data\": [[[1,2],[3,4]],"
            + "[[\"a\",\"b\"],[\"c\",\"d\"]]]}"));
    records.add(createRecord(12, 12, "multi-dimensional-2 array",
        "{\"name\": {\"first\": \"multi-dimensional-2\",\"last\": \"array\"},\"id\": 111,\"data\": [[[1,2],[3,4]],"
            + "[[\"a\",\"b\"],[\"c\",\"d\"]]]}"));
    records.add(createRecord(13, 13, "multi-dimensional-1 array",
        "{\"name\": {\"first\": \"multi-dimensional-1\",\"last\": \"array\"},\"id\": 111,\"data\": [[[1,2],[3,4]],"
            + "[[\"a\",\"b\"],[\"c\",\"d\"]]]}"));
    records.add(createRecord(13, 13, "days",
        "{\"name\": {\"first\": \"multi-dimensional-1\",\"last\": \"array\"},\"days\": 111}"));
    records.add(createRecord(14, 14, "top level array", "[{\"i1\":1,\"i2\":2}, {\"i1\":3,\"i2\":4}]"));

    records.add(createRecord(15, 15, "john doe", "{\"longVal\": \"9223372036854775807\"}"));
    records.add(createRecord(16, 16, "john doe", "{\"longVal\": \"-9223372036854775808\" }"));
    records.add(createRecord(17, 17, "john doe", "{\"longVal\": \"-100.12345\" }"));
    records.add(createRecord(18, 18, "john doe", "{\"longVal\": \"10e2\" }"));

    tableConfig.getIndexingConfig().setJsonIndexColumns(List.of("jsonColumn"));
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(indexDir.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, schema);
    ImmutableSegment immutableSegment =
        ImmutableSegmentLoader.load(new File(indexDir, SEGMENT_NAME), indexLoadingConfig, null);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

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

  protected void checkResult(String query, Object[][] expectedResults) {
    BrokerResponseNative brokerResponse = getBrokerResponseForOptimizedQuery(query, tableConfig(), schema());
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, Arrays.asList(expectedResults));
  }

  int numRecords() {
    return 10;
  }

  abstract TableConfig tableConfig();

  abstract Schema schema();

  File indexDir() {
    return new File(FileUtils.getTempDirectory(), getClass().getSimpleName());
  }

  abstract GenericRow createRecord(int intValue, long longValue, String stringValue, String jsonValue);
}
