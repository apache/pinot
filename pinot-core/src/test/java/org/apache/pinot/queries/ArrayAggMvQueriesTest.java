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
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
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


public class ArrayAggMvQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "ArrayAggMvQueriesTest");
  private static final String RAW_TABLE_NAME = "testTableMv";
  private static final String SEGMENT_NAME = "testSegment";

  private static final int NUM_RECORDS = 2000;

  private static final String INT_MV = "intMV";
  private static final String LONG_MV = "longMV";
  private static final String FLOAT_MV = "floatMV";
  private static final String DOUBLE_MV = "doubleMV";
  private static final String STRING_MV = "stringMV";
  private static final String GROUP_BY_COLUMN = "groupKey";

  private static final Schema SCHEMA = new Schema.SchemaBuilder().addMultiValueDimension(INT_MV, DataType.INT)
      .addMultiValueDimension(LONG_MV, DataType.LONG).addMultiValueDimension(FLOAT_MV, DataType.FLOAT)
      .addMultiValueDimension(DOUBLE_MV, DataType.DOUBLE).addMultiValueDimension(STRING_MV, DataType.STRING)
      .addSingleValueDimension(GROUP_BY_COLUMN, DataType.STRING).build();

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

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);

    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRow record = new GenericRow();
      record.putValue(INT_MV, new Integer[]{i, i + NUM_RECORDS + 1});
      record.putValue(LONG_MV, new Long[]{(long) i, (long) i + NUM_RECORDS + 1});
      record.putValue(FLOAT_MV, new Float[]{(float) i, (float) i + NUM_RECORDS + 1});
      record.putValue(DOUBLE_MV, new Double[]{(double) i, (double) i + NUM_RECORDS + 1});
      record.putValue(STRING_MV, new String[]{Integer.toString(i), Integer.toString(i + NUM_RECORDS + 1)});
      record.putValue(GROUP_BY_COLUMN, String.valueOf(i % 10));
      records.add(record);
    }

    SegmentGeneratorConfig conf =
        new SegmentGeneratorConfig(new TableConfigBuilder(org.apache.pinot.spi.config.table.TableType.OFFLINE)
            .setTableName(RAW_TABLE_NAME).build(), SCHEMA);
    conf.setTableName(RAW_TABLE_NAME);
    conf.setSegmentName(SEGMENT_NAME);
    conf.setOutDir(INDEX_DIR.getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(conf, new GenericRowRecordReader(records));
    driver.build();

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  @Test
  public void testArrayAggMvNonDistinct() {
    String query = "SELECT ArrayAgg(intMV, 'INT'), ArrayAgg(longMV, 'LONG'), ArrayAgg(floatMV, 'FLOAT'), "
        + "ArrayAgg(doubleMV, 'DOUBLE'), ArrayAgg(stringMV, 'STRING') FROM testTableMv";

    AggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    List<Object> aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    for (int i = 0; i < 5; i++) {
      assertEquals(((List<?>) aggregationResult.get(i)).size(), 2 * NUM_RECORDS);
    }

    ResultTable resultTable = getBrokerResponse(query).getResultTable();
    assertEquals(resultTable.getRows().get(0).length, 5);
    // Final result flattens MV values across both segments; with this setup it equals 8 × NUM_RECORDS
    assertEquals(((int[]) resultTable.getRows().get(0)[0]).length, 8 * NUM_RECORDS);
    assertEquals(((long[]) resultTable.getRows().get(0)[1]).length, 8 * NUM_RECORDS);
    assertEquals(((float[]) resultTable.getRows().get(0)[2]).length, 8 * NUM_RECORDS);
    assertEquals(((double[]) resultTable.getRows().get(0)[3]).length, 8 * NUM_RECORDS);
    assertEquals(((String[]) resultTable.getRows().get(0)[4]).length, 8 * NUM_RECORDS);
  }

  @Test
  public void testArrayAggMvDistinct() {
    String query = "SELECT ArrayAgg(intMV, 'INT', true), ArrayAgg(longMV, 'LONG', true), "
        + "ArrayAgg(floatMV, 'FLOAT', true), ArrayAgg(doubleMV, 'DOUBLE', true), "
        + "ArrayAgg(stringMV, 'STRING', true) FROM testTableMv";

    AggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    List<Object> aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    for (int i = 0; i < 5; i++) {
      assertEquals(((Set<?>) aggregationResult.get(i)).size(), 2 * NUM_RECORDS);
    }

    ResultTable resultTable = getBrokerResponse(query).getResultTable();
    assertEquals(resultTable.getRows().get(0).length, 5);
    assertEquals(((int[]) resultTable.getRows().get(0)[0]).length, 2 * NUM_RECORDS);
    assertEquals(((long[]) resultTable.getRows().get(0)[1]).length, 2 * NUM_RECORDS);
    assertEquals(((float[]) resultTable.getRows().get(0)[2]).length, 2 * NUM_RECORDS);
    assertEquals(((double[]) resultTable.getRows().get(0)[3]).length, 2 * NUM_RECORDS);
    assertEquals(((String[]) resultTable.getRows().get(0)[4]).length, 2 * NUM_RECORDS);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
