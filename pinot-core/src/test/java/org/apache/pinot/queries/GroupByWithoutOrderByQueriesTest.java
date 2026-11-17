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
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.rewriter.OrderByGroupKeysRewriter;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// A GROUP BY without ORDER BY on two servers that build different groups first. Each server runs its two segments one
/// after the other (`maxExecutionThreads = 1`), so without an ORDER BY it keeps the `LIMIT` groups of its first segment
/// and drops the new groups of the second one: the first server keeps keys 0 to 9, the second server keeps keys 10 to
/// 19. Both servers have every key, so each group the broker keeps has the values of one server only. Ordered on the
/// group keys by [OrderByGroupKeysRewriter], as the broker sends it, both servers keep keys 0 to 9, and the broker
/// returns them with the values of both servers.
public class GroupByWithoutOrderByQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "GroupByWithoutOrderByQueriesTest-" + UUID.randomUUID());
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String KEY_COLUMN = "groupKey";
  private static final String AMOUNT_COLUMN = "amount";
  private static final int LIMIT = 10;
  private static final long FIRST_SERVER_AMOUNT = 1;
  private static final long SECOND_SERVER_AMOUNT = 100;
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
      .addSingleValueDimension(KEY_COLUMN, DataType.INT)
      .addMetric(AMOUNT_COLUMN, DataType.LONG)
      .build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

  private List<List<IndexSegment>> _instances;

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _instances.get(0).get(0);
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    List<IndexSegment> indexSegments = new ArrayList<>();
    _instances.forEach(indexSegments::addAll);
    return indexSegments;
  }

  @Override
  protected List<List<IndexSegment>> getDistinctInstances() {
    return _instances;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    _instances = List.of(
        List.of(createSegment("firstServerKeys0To9", 0, FIRST_SERVER_AMOUNT),
            createSegment("firstServerKeys10To19", LIMIT, FIRST_SERVER_AMOUNT)),
        List.of(createSegment("secondServerKeys10To19", LIMIT, SECOND_SERVER_AMOUNT),
            createSegment("secondServerKeys0To9", 0, SECOND_SERVER_AMOUNT)));
  }

  /// Creates a segment with one row for each of the `LIMIT` keys from `firstKey`, each with the given amount.
  private static IndexSegment createSegment(String segmentName, int firstKey, long amount)
      throws Exception {
    List<GenericRow> rows = new ArrayList<>(LIMIT);
    for (int key = firstKey; key < firstKey + LIMIT; key++) {
      GenericRow row = new GenericRow();
      row.putValue(KEY_COLUMN, key);
      row.putValue(AMOUNT_COLUMN, amount);
      rows.add(row);
    }
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(segmentName);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(rows));
    driver.build();
    return ImmutableSegmentLoader.load(new File(INDEX_DIR, segmentName), ReadMode.mmap);
  }

  @Test
  public void testEveryServerKeepsTheSmallestKeys() {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SET maxExecutionThreads = 1; SELECT groupKey, SUM(amount) FROM testTable GROUP BY groupKey LIMIT " + LIMIT);
    new OrderByGroupKeysRewriter().rewrite(pinotQuery);
    BrokerResponseNative brokerResponse = getBrokerResponse(pinotQuery, PLAN_MAKER);
    assertTrue(brokerResponse.getExceptions().isEmpty(), brokerResponse.getExceptions().toString());
    List<Object[]> rows = brokerResponse.getResultTable().getRows();
    assertEquals(rows.size(), LIMIT);
    for (int key = 0; key < LIMIT; key++) {
      assertEquals(rows.get(key), new Object[]{key, (double) (FIRST_SERVER_AMOUNT + SECOND_SERVER_AMOUNT)});
    }
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    for (List<IndexSegment> indexSegments : _instances) {
      indexSegments.forEach(IndexSegment::destroy);
    }
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
