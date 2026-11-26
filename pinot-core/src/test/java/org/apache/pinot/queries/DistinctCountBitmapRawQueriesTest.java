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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


/// Tests bitmap distinct aggregation on raw and dictionary-encoded values across two segments.
/// The test instance owns its segments and is not thread-safe.
public class DistinctCountBitmapRawQueriesTest extends BaseQueriesTest {
  private static final String TABLE_NAME = "bitmapRawTestTable";
  private static final String GROUP_COLUMN = "groupCol";
  private static final String RAW_INT_COLUMN = "rawInt";
  private static final String RAW_DOUBLE_COLUMN = "rawDouble";
  private static final String DICT_DOUBLE_COLUMN = "dictDouble";
  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension(GROUP_COLUMN, DataType.STRING)
      .addSingleValueDimension(RAW_INT_COLUMN, DataType.INT)
      .addSingleValueDimension(RAW_DOUBLE_COLUMN, DataType.DOUBLE)
      .addSingleValueDimension(DICT_DOUBLE_COLUMN, DataType.DOUBLE)
      .build();
  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName(TABLE_NAME)
      .setNoDictionaryColumns(List.of(RAW_INT_COLUMN, RAW_DOUBLE_COLUMN))
      .build();

  private final List<IndexSegment> _segments = new ArrayList<>();
  private File _indexDir;

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _segments.get(0);
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _segments;
  }

  @Override
  protected List<List<IndexSegment>> getDistinctInstances() {
    return List.of(List.of(_segments.get(0)), List.of(_segments.get(1)));
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    _indexDir = Files.createTempDirectory("pinot-bitmap-raw-").toFile();
    _segments.add(buildSegment("segmentA", List.of(
        row("a", 10, 1.0, 10.0), row("a", 10, 2.0, 10.0), row("a", 20, 3.0, 20.0), row("b", 30, 3.0, 30.0))));
    _segments.add(buildSegment("segmentB", List.of(
        row("a", 20, 3.0, 20.0), row("a", 30, 4.0, 20.0), row("b", 30, 3.0, 30.0), row("b", 40, 3.0, 30.0))));

    for (IndexSegment segment : _segments) {
      assertNull(segment.getDataSource(RAW_INT_COLUMN).getDictionary());
      assertNull(segment.getDataSource(RAW_DOUBLE_COLUMN).getDictionary());
      assertNotNull(segment.getDataSource(DICT_DOUBLE_COLUMN).getDictionary());
    }
    // Dict id 0 means a different value in each segment. A merge must use values, not bare dictionary ids.
    assertEquals(_segments.get(0).getDataSource(DICT_DOUBLE_COLUMN).getDictionary().getDoubleValue(0), 10.0d);
    assertEquals(_segments.get(1).getDataSource(DICT_DOUBLE_COLUMN).getDictionary().getDoubleValue(0), 20.0d);
  }

  private static GenericRow row(String group, int rawInt, double rawDouble, double dictDouble) {
    GenericRow record = new GenericRow();
    record.putValue(GROUP_COLUMN, group);
    record.putValue(RAW_INT_COLUMN, rawInt);
    record.putValue(RAW_DOUBLE_COLUMN, rawDouble);
    record.putValue(DICT_DOUBLE_COLUMN, dictDouble);
    return record;
  }

  private IndexSegment buildSegment(String segmentName, List<GenericRow> records)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    config.setTableName(TABLE_NAME);
    config.setSegmentName(segmentName);
    config.setOutDir(_indexDir.getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(records));
    driver.build();
    return ImmutableSegmentLoader.load(new File(_indexDir, segmentName), ReadMode.mmap);
  }

  @Test
  public void testGroupByMergesRawAndDictionaryValuesAcrossSegments() {
    String query = "SELECT groupCol, DISTINCTCOUNTBITMAP(rawInt), DISTINCTCOUNTBITMAP(rawDouble), "
        + "DISTINCTCOUNTBITMAP(dictDouble) FROM " + TABLE_NAME + " GROUP BY groupCol ORDER BY groupCol";
    BrokerResponseNative response = getBrokerResponse(query);
    assertEquals(response.getExceptions().size(), 0, response.getExceptions().toString());
    assertNotNull(response.getResultTable());
    List<Object[]> rows = response.getResultTable().getRows();
    assertEquals(rows.size(), 2);
    assertGroupRow(rows.get(0), "a", 3, 4, 2);
    assertGroupRow(rows.get(1), "b", 2, 1, 1);
  }

  private static void assertGroupRow(Object[] row, String group, int intCount, int rawDoubleCount,
      int dictDoubleCount) {
    assertEquals(row.length, 4);
    assertEquals(row[0], group);
    assertEquals(((Number) row[1]).intValue(), intCount);
    assertEquals(((Number) row[2]).intValue(), rawDoubleCount);
    assertEquals(((Number) row[3]).intValue(), dictDoubleCount);
  }

  @Test
  public void testAggregationMergesRawAndDictionaryValuesAcrossSegments() {
    String query = "SELECT DISTINCTCOUNTBITMAP(rawInt), DISTINCTCOUNTBITMAP(rawDouble), "
        + "DISTINCTCOUNTBITMAP(dictDouble) FROM " + TABLE_NAME;
    BrokerResponseNative response = getBrokerResponse(query);
    assertEquals(response.getExceptions().size(), 0, response.getExceptions().toString());
    assertNotNull(response.getResultTable());
    List<Object[]> rows = response.getResultTable().getRows();
    assertEquals(rows.size(), 1);
    Object[] row = rows.get(0);
    assertEquals(row.length, 3);
    assertEquals(((Number) row[0]).intValue(), 4);
    assertEquals(((Number) row[1]).intValue(), 4);
    assertEquals(((Number) row[2]).intValue(), 3);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws IOException {
    try {
      for (IndexSegment segment : _segments) {
        segment.destroy();
      }
    } finally {
      if (_indexDir != null) {
        FileUtils.deleteDirectory(_indexDir);
      }
    }
  }
}
