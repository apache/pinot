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
package com.linkedin.pinot.query.transform;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.data.readers.GenericRowRecordReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.operator.BReusableFilteredDocIdSetOperator;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.blocks.TransformBlock;
import com.linkedin.pinot.core.operator.filter.MatchEntireSegmentOperator;
import com.linkedin.pinot.core.operator.transform.TransformExpressionOperator;
import com.linkedin.pinot.core.operator.transform.function.AdditionTransform;
import com.linkedin.pinot.core.operator.transform.function.DivisionTransform;
import com.linkedin.pinot.core.operator.transform.function.MultiplicationTransform;
import com.linkedin.pinot.core.operator.transform.function.SubtractionTransform;
import com.linkedin.pinot.core.operator.transform.function.TransformFunctionFactory;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Test for {@link TransformExpressionOperator}
 */
public class TransformExpressionOperatorTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(TransformExpressionOperatorTest.class);

  private static final String SEGMENT_DIR_NAME = System.getProperty("java.io.tmpdir") + File.separator + "xformSegDir";
  private static final String SEGMENT_NAME = "xformSeg";

  private static final int NUM_METRICS = 3;
  private static final long RANDOM_SEED = System.nanoTime();
  private static final int NUM_ROWS = DocIdSetPlanNode.MAX_DOC_PER_CALL;
  private static final double EPSILON = 1e-5;
  private static final int MAX_METRIC_VALUE = 1000;

  private Map<String, TestTransform> _transformMap;
  private IndexSegment _indexSegment;
  private double[][] _values;

  @BeforeClass
  public void setup()
      throws Exception {
    TransformFunctionFactory.init(
        new String[]{AdditionTransform.class.getName(), SubtractionTransform.class.getName(), MultiplicationTransform.class.getName(), DivisionTransform.class.getName()});

    Schema schema = buildSchema(NUM_METRICS);
    buildSegment(SEGMENT_DIR_NAME, SEGMENT_NAME, schema);
    _indexSegment = Loaders.IndexSegment.load(new File(SEGMENT_DIR_NAME, SEGMENT_NAME), ReadMode.heap);

    _transformMap = new HashMap<>();
    _transformMap = buildTransformMap();
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(new File(SEGMENT_DIR_NAME));
  }

  @Test
  public void test() {

    for (Map.Entry<String, TestTransform> entry : _transformMap.entrySet()) {
      String expression = entry.getKey();
      TestTransform xform = entry.getValue();

      double[] actual = evaluateExpression(expression);
      for (int i = 0; i < actual.length; i++) {
        Assert.assertEquals(actual[i], xform.compute(_values[i]), EPSILON, "Expression: " + expression);
      }
    }
  }

  /**
   * Helper method to evaluate one expression using the TransformOperator.
   * @param expression Expression to evaluate
   * @return Result of evaluation
   */
  private double[] evaluateExpression(String expression) {

    Operator filterOperator = new MatchEntireSegmentOperator(_indexSegment.getSegmentMetadata().getTotalDocs());
    final BReusableFilteredDocIdSetOperator docIdSetOperator =
        new BReusableFilteredDocIdSetOperator(filterOperator, _indexSegment.getSegmentMetadata().getTotalDocs(),
            NUM_ROWS);
    final Map<String, BaseOperator> dataSourceMap = buildDataSourceMap(_indexSegment.getSegmentMetadata().getSchema());

    final MProjectionOperator projectionOperator = new MProjectionOperator(dataSourceMap, docIdSetOperator);

    Pql2Compiler compiler = new Pql2Compiler();
    List<TransformExpressionTree> expressionTrees = new ArrayList<>(1);
    expressionTrees.add(compiler.compileToExpressionTree(expression));

    TransformExpressionOperator transformOperator =
        new TransformExpressionOperator(projectionOperator, expressionTrees);
    TransformBlock transformBlock = transformOperator.nextBlock();
    BlockValSet blockValueSet = transformBlock.getBlockValueSet(expression);
    return blockValueSet.getDoubleValuesSV();
  }

  /**
   * Helper method to build a segment with {@link #NUM_METRICS} metrics with random
   * data as per the schema.
   *
   * @param segmentDirName Name of segment directory
   * @param segmentName Name of segment
   * @param schema Schema for segment
   * @return Schema built for the segment
   * @throws Exception
   */
  private Schema buildSegment(String segmentDirName, String segmentName, Schema schema)
      throws Exception {

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);
    config.setOutDir(segmentDirName);
    config.setFormat(FileFormat.AVRO);
    config.setSegmentName(segmentName);

    Random random = new Random(RANDOM_SEED);

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    _values = new double[NUM_ROWS][NUM_METRICS];
    for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
      HashMap<String, Object> map = new HashMap<>();

      // Metric columns.
      for (int i = 0; i < NUM_METRICS; i++) {
        String metName = schema.getMetricFieldSpecs().get(i).getName();
        double value = random.nextInt(MAX_METRIC_VALUE) + random.nextDouble() + 1.0;
        map.put(metName, value);
        _values[rowId][i] = value;
      }

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      rows.add(genericRow);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows, schema));
    driver.build();

    LOGGER.info("Built segment {} at {}", segmentName, segmentDirName);
    return schema;
  }

  /**
   * Helper method to build a schema with provided number of metric columns.
   *
   * @param numMetrics Number of metric columns in the schema
   * @return Schema containing the given number of metric columns
   */
  private static Schema buildSchema(int numMetrics) {
    Schema schema = new Schema();
    for (int i = 0; i < numMetrics; i++) {
      String metricName = "m_" + i;
      MetricFieldSpec metricFieldSpec = new MetricFieldSpec(metricName, FieldSpec.DataType.DOUBLE);
      schema.addField(metricFieldSpec);
    }
    return schema;
  }

  /**
   * Helper method to build data source map for all the metric columns.
   *
   * @param schema Schema for the index segment
   * @return Map of metric name to its data source.
   */
  private Map<String, BaseOperator> buildDataSourceMap(Schema schema) {
    final Map<String, BaseOperator> dataSourceMap = new HashMap<>();
    for (String metricName : schema.getMetricNames()) {
      dataSourceMap.put(metricName, _indexSegment.getDataSource(metricName));
    }
    return dataSourceMap;
  }

  /**
   * Helper method that builds a map from an expression to a way to evaluate
   * the expression.
   *
   * @return Map containing expression to its evaluator.
   */
  private Map<String, TestTransform> buildTransformMap() {
    Map<String, TestTransform> transformMap = new HashMap<>();
    transformMap.put("sub(add(m_1, m_2), m_0)", new TestTransform() {

      @Override
      public double compute(double... values) {
        return (values[1] + values[2] - values[0]);
      }
    });

    transformMap.put("sub(mult(m_2, m_1), m_0)", new TestTransform() {
      @Override
      public double compute(double... values) {
        return ((values[2] * values[1]) - values[0]);
      }
    });

    transformMap.put("div(add(m_0, m_2), add(m_1, m_2))", new TestTransform() {
      @Override
      public double compute(double... values) {
        return ((values[0] + values[2]) / (values[1] + values[2]));
      }
    });

    transformMap.put("div(mult(add(m_0, m_1), m_2), sub(m_1, m_2))", new TestTransform() {
      @Override
      public double compute(double... values) {
        return (((values[0] + values[1]) * values[2]) / (values[1] - values[2]));
      }
    });

    return transformMap;
  }

  /**
   * Interface for test transform.
   */
  private interface TestTransform {
    double compute(double... values);
  }
}
