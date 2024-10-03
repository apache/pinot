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
import java.util.HashMap;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey.MAX_QUERY_RESPONSE_SIZE_BYTES;
import static org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey.MAX_SERVER_RESPONSE_SIZE_BYTES;
import static org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey.TIMEOUT_MS;


public class WithOptionQueriesTest extends BaseQueriesTest {

  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "WithOptionQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final int NUM_RECORDS = 10;
  private static final String X_COL = "x";
  private static final String Y_COL = "y";

  private static final Schema SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension(X_COL, FieldSpec.DataType.INT)
      .addSingleValueDimension(Y_COL, FieldSpec.DataType.DOUBLE).build();

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

  private final List<Object[]> _allRecords = new ArrayList<>();

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRow record = new GenericRow();
      record.putValue(X_COL, i);
      record.putValue(Y_COL, 0.25);
      records.add(record);
      _allRecords.add(new Object[]{i, 0.25});
    }

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  @Test
  public void testOptionParsingFailure() {
    HashMap<String, String> options = new HashMap<>();

    // int values
    for (String setting : Arrays.asList(QueryOptionKey.NUM_GROUPS_LIMIT,
        QueryOptionKey.MAX_INITIAL_RESULT_HOLDER_CAPACITY, QueryOptionKey.GROUP_TRIM_THRESHOLD,
        QueryOptionKey.MAX_EXECUTION_THREADS,
        QueryOptionKey.MIN_SEGMENT_GROUP_TRIM_SIZE, QueryOptionKey.MIN_SERVER_GROUP_TRIM_SIZE,
        QueryOptionKey.MIN_BROKER_GROUP_TRIM_SIZE)) {

      options.clear();
      for (String value : new String[]{"-10000000000", "-2147483648", "-1", "2147483648", "10000000000"}) {
        options.put(setting, value);

        Assert.assertThrows(setting + " must be a number between 0 and 2^31-1, got: " + value, );

        try {
          getBrokerResponse("SELECT x, count(*) FROM " + RAW_TABLE_NAME + " GROUP BY x", options);
          Assert.fail("Error expected for " + setting + "=" + value);
        } catch (IllegalStateException ise) {
          Assert.assertEquals(ise.getMessage(), );
        }
      }
    }
  }

  @Test
  public void testOptionParsingSuccess() {
    HashMap<String, String> options = new HashMap<>();
    List<Object> groupRows = new ArrayList();
    groupRows.add(new Object[]{0d, 40L}); //four times 10 records because segment gets multiplied under the hood

    // int values
    for (String setting : Arrays.asList(/*QueryOptionKey.NUM_GROUPS_LIMIT,*/
        QueryOptionKey.MAX_INITIAL_RESULT_HOLDER_CAPACITY, QueryOptionKey.MULTI_STAGE_LEAF_LIMIT,
        QueryOptionKey.GROUP_TRIM_THRESHOLD, QueryOptionKey.MAX_STREAMING_PENDING_BLOCKS,
        QueryOptionKey.MAX_ROWS_IN_JOIN, QueryOptionKey.MAX_EXECUTION_THREADS,
        QueryOptionKey.MIN_SEGMENT_GROUP_TRIM_SIZE, QueryOptionKey.MIN_SERVER_GROUP_TRIM_SIZE,
        QueryOptionKey.MIN_BROKER_GROUP_TRIM_SIZE, QueryOptionKey.NUM_REPLICA_GROUPS_TO_QUERY)) {

      options.clear();
      for (String value : new String[]{"0", "1", "10000", "2147483647"}) {
        options.put(setting, value);
        List<Object[]> rows =
            getBrokerResponse("SELECT mod(x,1), count(*) FROM " + RAW_TABLE_NAME + " GROUP BY mod(x,1)",
                options).getResultTable().getRows();
        assertEquals(rows, groupRows);
      }
    }

    //long values
    for (String setting : Arrays.asList(TIMEOUT_MS, MAX_SERVER_RESPONSE_SIZE_BYTES, MAX_QUERY_RESPONSE_SIZE_BYTES)) {
      options.clear();
      for (String value : new String[]{"1", "10000", "9223372036854775807"}) {
        options.put(setting, value);
        List<Object[]> rows = getBrokerResponse("SELECT * FROM " + RAW_TABLE_NAME, options).getResultTable().getRows();
        assertEquals(rows, _allRecords);
      }
    }
  }

  private void assertEquals(List actual, List expected) {
    if (actual == expected) {
      return;
    }

    if (actual == null || expected == null || actual.size() != expected.size()) {
      Assert.fail("Expected \n" + expected + "\n but got \n" + actual);
    }

    for (int i = 0; i < actual.size(); i++) {
      Object act = actual.get(i);
      Object exp = expected.get(i);

      if (act instanceof Object[]) {
        Assert.assertEquals((Object[]) act, (Object[]) exp);
      } else {
        Assert.assertEquals(act, exp);
      }
    }
  }
}
