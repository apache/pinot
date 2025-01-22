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
import org.apache.pinot.segment.local.segment.creator.impl.ColumnJsonParserException;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class JsonMalformedIndexTest extends BaseQueriesTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String STRING_COLUMN = "stringColumn";
  private static final String JSON_COLUMN = "jsonColumn";
  //@formatter:off
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
      .addSingleValueDimension(STRING_COLUMN, DataType.STRING)
      .addSingleValueDimension(JSON_COLUMN, DataType.STRING)
      .build();
  //@formatter:on
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setJsonIndexColumns(List.of(JSON_COLUMN))
          .build();
  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;
  private final List<GenericRow> _records = new ArrayList<>();

  @BeforeClass
  public void setUp()
      throws Exception {
    _records.add(createRecord("ludwik von drake",
        "{\"name\": {\"first\": \"ludwik\", \"last\": \"von drake\"}, \"id\": 181, "
            + "\"data\": [\"l\", \"b\", \"c\", \"d\"]"));
  }

  protected void checkResult(String query, Object[][] expectedResults) {
    BrokerResponseNative brokerResponse = getBrokerResponseForOptimizedQuery(query, TABLE_CONFIG, SCHEMA);
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, Arrays.asList(expectedResults));
  }

  File indexDir() {
    return new File(FileUtils.getTempDirectory(), getClass().getSimpleName());
  }

  GenericRow createRecord(String stringValue, String jsonValue) {
    GenericRow record = new GenericRow();
    record.putValue(STRING_COLUMN, stringValue);
    record.putValue(JSON_COLUMN, jsonValue);
    return record;
  }

  @Test(expectedExceptions = ColumnJsonParserException.class, expectedExceptionsMessageRegExp = "Column: jsonColumn.*")
  public void testJsonIndexBuild()
      throws Exception {
    File indexDir = indexDir();
    FileUtils.deleteDirectory(indexDir);

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setOutDir(indexDir.getPath());
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(_records));
    driver.build();

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(TABLE_CONFIG, SCHEMA);
    ImmutableSegment segment = ImmutableSegmentLoader.load(new File(indexDir, SEGMENT_NAME), indexLoadingConfig,
        null);
    _indexSegment = segment;
    _indexSegments = List.of(segment, segment);

    Object[][] expected = {{"von drake"}, {"von drake"}, {"von drake"}, {"von drake"}};
    checkResult("SELECT jsonextractscalar(jsonColumn, '$.name.last', 'STRING') FROM testTable", expected);
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
}
