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


public class ListAggQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "ListAggQueriesTest");
  private static final String RAW_TABLE_NAME = "testTableListAgg";
  private static final String SEGMENT_NAME = "testSegment";

  private static final int NUM_RECORDS = 200;

  private static final String STR_MV = "strMV";
  private static final String STR_SV = "strSV";
  private static final String GROUP_BY_COLUMN = "groupKey";

  private static final Schema SCHEMA = new Schema.SchemaBuilder().addMultiValueDimension(STR_MV, DataType.STRING)
      .addSingleValueDimension(STR_SV, DataType.STRING)
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
      record.putValue(STR_MV, new String[]{"A", (i % 2 == 0) ? "B" : "C"});
      record.putValue(STR_SV, (i % 2 == 0) ? "X" : "Y");
      record.putValue(GROUP_BY_COLUMN, String.valueOf(i % 10));
      records.add(record);
    }

    SegmentGeneratorConfig conf = new SegmentGeneratorConfig(
        new TableConfigBuilder(org.apache.pinot.spi.config.table.TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
            .build(), SCHEMA);
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
  public void testListAggNonDistinct() {
    String q = "SELECT listAgg(strMV, ',') FROM testTableListAgg";
    AggregationOperator op = getOperator(q);
    AggregationResultsBlock block = op.nextBlock();
    List<Object> res = block.getResults();
    assertNotNull(res);
    // Each row contributes 2 elements; inner-segment result is the intermediate collection
    Object partial = res.get(0);
    assertEquals(((java.util.Collection<?>) partial).size(), 2 * NUM_RECORDS);

    ResultTable table = getBrokerResponse(q).getResultTable();
    assertEquals(table.getRows().get(0).length, 1);
    // Each row has 2 MV values; with 2 segments and 2 servers, expect 8 * NUM_RECORDS
    assertEquals(((String) table.getRows().get(0)[0]).split(",").length, 8 * NUM_RECORDS);
  }

  @Test
  public void testListAggDistinct() {
    String q = "SELECT listAgg(strMV, ',', true) FROM testTableListAgg";
    AggregationOperator op = getOperator(q);
    AggregationResultsBlock block = op.nextBlock();
    List<Object> res = block.getResults();
    assertNotNull(res);
    // Distinct values are {A,B,C}; inner-segment result is the intermediate set
    Object partial = res.get(0);
    assertEquals(((java.util.Collection<?>) partial).size(), 3);

    // Inter-segment (broker) result is the final string
    ResultTable table = getBrokerResponse(q).getResultTable();
    assertEquals(((String) table.getRows().get(0)[0]).split(",").length, 3);
  }

  @Test
  public void testListAggExplicitFalseOnMV() {
    String q = "SELECT listAgg(strMV, ',', false) FROM testTableListAgg";
    AggregationOperator op = getOperator(q);
    AggregationResultsBlock block = op.nextBlock();
    List<Object> res = block.getResults();
    assertNotNull(res);
    Object partial = res.get(0);
    assertEquals(((java.util.Collection<?>) partial).size(), 2 * NUM_RECORDS);

    ResultTable table = getBrokerResponse(q).getResultTable();
    assertEquals(((String) table.getRows().get(0)[0]).split(",").length, 8 * NUM_RECORDS);
  }

  @Test
  public void testListAggSVNonDistinct() {
    String q = "SELECT listAgg(strSV, '|') FROM testTableListAgg";
    AggregationOperator op = getOperator(q);
    AggregationResultsBlock block = op.nextBlock();
    List<Object> res = block.getResults();
    assertNotNull(res);
    Object partial = res.get(0);
    assertEquals(((java.util.Collection<?>) partial).size(), NUM_RECORDS);

    ResultTable table = getBrokerResponse(q).getResultTable();
    assertEquals(((String) table.getRows().get(0)[0]).split("\\|").length, 4 * NUM_RECORDS);
  }

  @Test
  public void testListAggSVDistinct() {
    String q = "SELECT listAgg(strSV, ',', true) FROM testTableListAgg";
    AggregationOperator op = getOperator(q);
    AggregationResultsBlock block = op.nextBlock();
    List<Object> res = block.getResults();
    assertNotNull(res);
    Object partial = res.get(0);
    // Distinct values are {X,Y}
    assertEquals(((java.util.Collection<?>) partial).size(), 2);

    ResultTable table = getBrokerResponse(q).getResultTable();
    assertEquals(((String) table.getRows().get(0)[0]).split(",").length, 2);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
