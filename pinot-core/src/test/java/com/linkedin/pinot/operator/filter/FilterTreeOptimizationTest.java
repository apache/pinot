/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.operator.filter;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.operator.blocks.BaseFilterBlock;
import com.linkedin.pinot.core.operator.filter.BaseFilterOperator;
import com.linkedin.pinot.core.plan.FilterPlanNode;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import com.linkedin.pinot.query.transform.TransformExpressionOperatorTest;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit test for optimization of predicates that evaluate to always false, during filter tree creation.
 */
public class FilterTreeOptimizationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransformExpressionOperatorTest.class);

  private static final String SEGMENT_DIR_NAME =
      System.getProperty("java.io.tmpdir") + File.separator + "filterOptimization";
  private static final String SEGMENT_NAME = "filterOptSeg";
  private static final String TABLE_NAME = "filterOptTable";

  private static final int NUM_ROWS = 1000;
  private static final String[] DIMENSIONS = new String[]{"dim_0", "dim_1", "dim_2", "dim_3"};
  private static final int MAX_DIMENSION_VALUES = 100;

  private IndexSegment _indexSegment;
  private Pql2Compiler _compiler;

  @BeforeClass
  public void setup()
      throws Exception {
    Schema schema = buildSchema(DIMENSIONS);
    buildSegment(SEGMENT_DIR_NAME, SEGMENT_NAME, schema);
    _indexSegment = Loaders.IndexSegment.load(new File(SEGMENT_DIR_NAME, SEGMENT_NAME), ReadMode.heap);
    _compiler = new Pql2Compiler();
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(new File(SEGMENT_DIR_NAME));
  }

  @Test
  public void test() {
    // No alwaysFalse predicates
    testQuery(
        String.format("select count(*) from %s where (%s = 'dim_0_1' AND %s <> 'dim_1_2')", TABLE_NAME, DIMENSIONS[0],
            DIMENSIONS[1]));

    // (nonAlwaysFalse AND (nonAlwaysFalse AND alwaysFalse)
    testQuery(
        String.format("select count(*) from %s where (%s = 'dim_0_1' AND (%s <> 'dim_1_2' AND %s = 'x'))", TABLE_NAME,
            DIMENSIONS[0], DIMENSIONS[1], DIMENSIONS[2]));

    // (alwaysFalse OR (alwaysFalse OR alwaysFalse))
    testQuery(
        String.format("select count(*) from %s where (%s = 'x' OR (%s ='y' OR %s = 'z'))", TABLE_NAME,
            DIMENSIONS[0], DIMENSIONS[1], DIMENSIONS[2]));

    // ((alwaysFalse AND nonAlwaysFalse) OR AlwaysFalse)
    testQuery(
        String.format("select count(*) from %s where ((%s = 'x' AND %s ='dim_1_5') OR %s = 'z')", TABLE_NAME,
            DIMENSIONS[0], DIMENSIONS[1], DIMENSIONS[2]));
  }

  /**
   * Helper method to perform the actual testing for the given query.
   * <ul>
   *   <li> Constructs the operator tree with and without alwaysFalse optimizations. </li>
   *   <li> Compares that all docIds filtered by the two operators are identical. </li>
   * </ul>
   * @param query Query to run.
   */
  private void testQuery(String query) {
    BrokerRequest brokerRequest = _compiler.compileToBrokerRequest(query);
    FilterQueryTree filterQueryTree = RequestUtils.generateFilterQueryTree(brokerRequest);

    BaseFilterOperator expectedOperator =
        FilterPlanNode.constructPhysicalOperator(filterQueryTree, _indexSegment, false);

    BaseFilterOperator actualOperator = FilterPlanNode.constructPhysicalOperator(filterQueryTree, _indexSegment, true);

    BaseFilterBlock expectedBlock;
    while ((expectedBlock = expectedOperator.getNextBlock()) != null) {
      BaseFilterBlock actualBlock = actualOperator.getNextBlock();
      Assert.assertNotNull(actualBlock);

      final BlockDocIdSet expectedDocIdSet = expectedBlock.getBlockDocIdSet();
      final BlockDocIdIterator expectedIterator = expectedDocIdSet.iterator();
      final BlockDocIdSet actualDocIdSet = actualBlock.getBlockDocIdSet();
      final BlockDocIdIterator actualIterator = actualDocIdSet.iterator();

      int expectedDocId;
      int actualDocId;
      while (((expectedDocId = expectedIterator.next()) != Constants.EOF) && ((actualDocId = actualIterator.next())
          != Constants.EOF)) {
        Assert.assertEquals(actualDocId, expectedDocId);
      }

      Assert.assertTrue(expectedIterator.next() == Constants.EOF);
      Assert.assertTrue(actualIterator.next() == Constants.EOF);
    }
  }

  /**
   * Helper method to build a segment.
   *
   * @param segmentDirName Name of segment directory
   * @param segmentName Name of segment
   * @param schema Schema for segment
   * @return Schema built for the segment
   * @throws Exception
   */
  private RecordReader buildSegment(String segmentDirName, String segmentName, Schema schema)
      throws Exception {

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);
    config.setOutDir(segmentDirName);
    config.setFormat(FileFormat.AVRO);
    config.setTableName(TABLE_NAME);
    config.setSegmentName(segmentName);

    final List<GenericRow> data = new ArrayList<>();
    for (int row = 0; row < NUM_ROWS; row++) {
      HashMap<String, Object> map = new HashMap<>();
      for (String dimensionName : DIMENSIONS) {
        map.put(dimensionName, dimensionName + '_' + (row % MAX_DIMENSION_VALUES));
      }

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      data.add(genericRow);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    RecordReader reader = new TestUtils.GenericRowRecordReader(schema, data);
    driver.init(config, reader);
    driver.build();

    LOGGER.info("Built segment {} at {}", segmentName, segmentDirName);
    return reader;
  }

  /**
   * Helper method to build a schema containing dimensions with names passed in.
   *
   * @param dimensions Dimension names
   *
   */
  private static Schema buildSchema(String[] dimensions) {
    Schema schema = new Schema();
    for (String dimension : dimensions) {
      DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec(dimension, FieldSpec.DataType.STRING, true);
      schema.addField(dimensionFieldSpec);
    }
    return schema;
  }
}
